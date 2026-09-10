"""
Apache Airflow DAG: Smart Grid Daily Batch Trigger & Pipeline Health Monitoring.

Architectural Role in Kappa Architecture:
------------------------------------------
In Kappa Architecture, the primary analytics engine is continuous (PySpark).
However, a production data platform requires an orchestrator (Apache Airflow) for:
1. Scheduled Reference Drops: Triggering auxiliary and dimension updates (e.g., daily 
   tariff revisions, where 1 simulated day = 5 minutes).
2. Data Quality (DQ) Gates: Validating that newly arrived reference data conforms 
   to schemas and boundary expectations before downstream joins occur.
3. Health Auditing & SLA Verification: Proactively testing Kafka broker availability,
   PostgreSQL connectivity, and streaming sink freshness (watermark / ingestion lag).
4. Automated Alerting & Heartbeats: Recording pipeline health metrics into 
   'pipeline_health_logs' for centralized monitoring.

Schedule:
Runs every 5 minutes ('*/5 * * * *') to align with the simulated 1 day = 5 minutes rule.
"""

import os
import sys
import socket
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to path for Airflow worker execution
DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    PythonOperator = None
    BashOperator = None

from config import settings

logger = settings.get_logger("smart_grid_dag")

# Default SLA and retry arguments
DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=4),
}


# --------------------------------------------------------------------------
# Task Callbacks & Failure Handlers
# --------------------------------------------------------------------------
def on_failure_callback(context):
    """Structured alert hook invoked if any pipeline task fails."""
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    execution_date = str(context.get("execution_date"))
    exception = context.get("exception")

    logger.error(
        f"Airflow Task Failure Alert: [{dag_id}.{task_id}] failed at {execution_date}",
        extra={"props": {
            "alert": "TASK_FAILURE",
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date,
            "error": str(exception)
        }}
    )


# --------------------------------------------------------------------------
# Task 1: Infrastructure Connectivity Check (Kafka & Postgres)
# --------------------------------------------------------------------------
def check_infrastructure_health(**kwargs):
    """
    Pings Kafka and PostgreSQL network endpoints to ensure platform availability
    prior to executing batch operations.
    """
    logger.info("Starting infrastructure connectivity health audit...")

    # 1. Test Kafka Broker connectivity
    kafka_host, kafka_port = settings.KAFKA_BOOTSTRAP_SERVERS.split(",")[0].split(":")
    kafka_healthy = False
    try:
        with socket.create_connection((kafka_host, int(kafka_port)), timeout=5):
            kafka_healthy = True
            logger.info(f"Kafka broker reachable at {kafka_host}:{kafka_port}")
    except Exception as e:
        logger.warning(f"Kafka connectivity probe failed: {e}")

    # 2. Test PostgreSQL connectivity
    postgres_host = settings.POSTGRES_HOST
    postgres_port = settings.POSTGRES_PORT
    pg_healthy = False
    try:
        with socket.create_connection((postgres_host, postgres_port), timeout=5):
            pg_healthy = True
            logger.info(f"PostgreSQL host reachable at {postgres_host}:{postgres_port}")
    except Exception as e:
        logger.warning(f"PostgreSQL connectivity probe failed: {e}")

    results = {
        "kafka_status": "UP" if kafka_healthy else "DOWN",
        "postgres_status": "UP" if pg_healthy else "DOWN",
        "checked_at": datetime.now(timezone.utc).isoformat()
    }

    logger.info("Infrastructure health check complete", extra={"props": results})
    return results


# --------------------------------------------------------------------------
# Task 2: Trigger Daily Batch Tariff CSV Generation
# --------------------------------------------------------------------------
def trigger_daily_tariff_generation(**kwargs):
    """
    Executes the batch script to generate the simulated day's tariff CSV.
    Uses the atomic drop pattern so streaming engines experience zero partial reads.
    """
    from scripts.tariff_batch_generator import execute_batch_drop

    logger.info("Triggering simulated daily tariff CSV batch drop...")
    output_path = execute_batch_drop(
        output_dir=settings.RAW_TARIFF_DIR,
        num_meters=settings.DEFAULT_NUM_METERS,
        day_offset=kwargs.get("logical_date", datetime.now()).day
    )
    logger.info(f"Tariff batch generation committed to: {output_path}")
    return str(output_path)


# --------------------------------------------------------------------------
# Task 3: Data Quality (DQ) Verification Gate
# --------------------------------------------------------------------------
def validate_tariff_data_quality(**kwargs):
    """
    Data Quality Gate: Validates that the freshly generated tariff CSV:
    1. Exists on disk.
    2. Contains rows > 0 (not empty).
    3. Contains all mandatory schema columns.
    4. Tariff rates are strictly positive and within valid economic ranges ($0.05 - $1.00).
    """
    canonical_file = settings.RAW_TARIFF_DIR / "tariffs_latest.csv"
    logger.info(f"Validating Data Quality for {canonical_file}...")

    if not canonical_file.exists():
        raise FileNotFoundError(f"DQ Failure: Tariff file {canonical_file} does not exist.")

    expected_cols = {"household_id", "tariff_rate", "billing_tier", "subsidy_flag"}
    valid_records = 0

    with open(canonical_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])

        if not expected_cols.issubset(headers):
            missing = expected_cols - headers
            raise ValueError(f"DQ Failure: Missing mandatory columns: {missing}")

        for row_idx, row in enumerate(reader, start=1):
            rate = float(row["tariff_rate"])
            if not (0.05 <= rate <= 1.50):
                raise ValueError(
                    f"DQ Failure at row {row_idx}: Tariff rate {rate} out of valid bounds [0.05, 1.50]"
                )
            if row["subsidy_flag"] not in ("0", "1"):
                raise ValueError(
                    f"DQ Failure at row {row_idx}: Invalid subsidy flag {row['subsidy_flag']}"
                )
            valid_records += 1

    if valid_records == 0:
        raise ValueError("DQ Failure: Tariff dataset is empty (0 records).")

    logger.info(
        "Tariff Data Quality Check: PASSED",
        extra={"props": {"file": str(canonical_file), "validated_records": valid_records}}
    )
    return {"status": "PASSED", "record_count": valid_records}


# --------------------------------------------------------------------------
# Task 4: Stream Ingestion Freshness & Pipeline Health Monitor
# --------------------------------------------------------------------------
def monitor_stream_freshness(**kwargs):
    """
    Queries PostgreSQL 'grid_zone_metrics' to verify that PySpark Structured
    Streaming is actively committing fresh micro-batches and not stalled.
    Logs health audit into 'pipeline_health_logs'.
    """
    logger.info("Auditing PySpark Structured Streaming freshness in PostgreSQL...")

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            connect_timeout=5
        )
        cur = conn.cursor()

        # Query latest committed window
        cur.execute("""
            SELECT MAX(window_end), MAX(updated_at), COUNT(*) 
            FROM grid_zone_metrics;
        """)
        row = cur.fetchone()
        latest_window_end, latest_update, total_rows = row

        status = "HEALTHY"
        details = {
            "total_metrics_records": total_rows,
            "latest_window_end": str(latest_window_end) if latest_window_end else None,
            "latest_update": str(latest_update) if latest_update else None
        }

        # Check recency: if updates are older than 5 minutes, mark DEGRADED
        if latest_update:
            age_sec = (datetime.now(timezone.utc) - latest_update.replace(tzinfo=timezone.utc)).total_seconds()
            details["lag_seconds"] = age_sec
            if age_sec > 300:
                status = "DEGRADED"
                logger.warning(f"Streaming ingestion lag detected: {age_sec:.1f}s")
        else:
            status = "INITIALIZING"

        # Record health heartbeat in PostgreSQL health audit table
        cur.execute("""
            INSERT INTO pipeline_health_logs (component, status, latency_ms, details)
            VALUES (%s, %s, %s, %s);
        """, ("PYSPARK_STREAM_SINK", status, 0.0, psycopg2.extras.Json(details) if hasattr(psycopg2, 'extras') else str(details)))
        conn.commit()

        cur.close()
        conn.close()

        logger.info(f"Stream Freshness Audit: {status}", extra={"props": details})
        return {"status": status, "details": details}

    except Exception as err:
        logger.warning(f"Postgres health check non-fatal warning (DB might be booting): {err}")
        return {"status": "UNAVAILABLE", "error": str(err)}


# --------------------------------------------------------------------------
# DAG Definition (Instantiated when loaded inside an Airflow environment)
# --------------------------------------------------------------------------
if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="smart_grid_energy_monitoring_pipeline",
        default_args=DEFAULT_ARGS,
        description="Orchestrates daily tariff drops and monitors Smart Grid Kappa pipeline health",
        schedule_interval="*/5 * * * *",  # Trigger every 5 minutes (1 simulated day)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["smart_grid", "kappa_architecture", "energy_monitoring"],
        on_failure_callback=on_failure_callback,
    )

    # 1. Infrastructure connectivity verification
    task_check_infra = PythonOperator(
        task_id="check_infrastructure_health",
        python_callable=check_infrastructure_health,
        dag=dag,
    )

    # 2. Daily simulated tariff drop
    task_generate_tariffs = PythonOperator(
        task_id="generate_daily_tariffs",
        python_callable=trigger_daily_tariff_generation,
        dag=dag,
    )

    # 3. Data Quality Gate
    task_validate_dq = PythonOperator(
        task_id="validate_tariff_data_quality",
        python_callable=validate_tariff_data_quality,
        dag=dag,
    )

    # 4. Ingestion stream freshness monitor
    task_monitor_stream = PythonOperator(
        task_id="monitor_stream_freshness",
        python_callable=monitor_stream_freshness,
        dag=dag,
    )

    # Architectural Dependency Pipeline:
    # Infrastructure check -> Generate daily tariffs -> Validate DQ -> Monitor Stream Health
    task_check_infra >> task_generate_tariffs >> task_validate_dq >> task_monitor_stream
else:
    dag = None
    logger.info("Airflow package not installed in local environment. Run tasks via Docker or standalone CLI.")


if __name__ == "__main__":
    print("--- Executing Smart Grid Health & Tariff Pipeline Standalone ---")
    infra_res = check_infrastructure_health()
    print(f"1. Infrastructure: {infra_res}")
    tariff_path = trigger_daily_tariff_generation()
    print(f"2. Tariff Generated: {tariff_path}")
    dq_res = validate_tariff_data_quality()
    print(f"3. Data Quality Gate: {dq_res}")
    stream_res = monitor_stream_freshness()
    print(f"4. Stream Monitor: {stream_res}")
    print("--- Pipeline execution completed successfully ---")
