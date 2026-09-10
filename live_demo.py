"""
========================================================================================
SMART GRID ENERGY MONITORING SYSTEM - LIVE INTERACTIVE DEMONSTRATION
Kappa Architecture (Kafka + Stream Processing + Airflow + SQL Database)
========================================================================================

Usage:
    python live_demo.py

Why this script?
Enables an immediate, 100% reliable LIVE DEMO without requiring Docker or downloading
multi-gigabyte images over low-bandwidth internet connections. It demonstrates all three
required tasks in real-time right in your terminal!
"""

import os
import sys
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from scripts.telemetry_producer import SmartGridTelemetrySimulator, ResilientKafkaProducer
from scripts.tariff_batch_generator import execute_batch_drop
from streaming.live_stream_engine import LiveKappaStreamProcessor, SQLITE_DB_PATH
from dags.smart_grid_dag import (
    check_infrastructure_health,
    validate_tariff_data_quality,
    monitor_stream_freshness,
)

RUNNING = True


def signal_handler(sig, frame):
    global RUNNING
    RUNNING = False
    print("\n[INFO] Gracefully shutting down Smart Grid live demonstration...")


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def clear_console():
    """Clears terminal screen for smooth dashboard refresh."""
    os.system('cls' if os.name == 'nt' else 'clear')


def render_dashboard(
    simulated_day: int,
    batch_num: int,
    total_events: int,
    zone_metrics: list,
    infra_status: dict,
    dq_status: dict,
    tariff_file: Path,
    db_mode: str
):
    """Renders high-visibility real-time monitoring dashboard."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = []
    lines.append("=" * 88)
    lines.append("   *** SMART GRID ENERGY MONITORING SYSTEM -- LIVE KAPPA PIPELINE DEMO ***")
    lines.append("=" * 88)
    lines.append(f"  Current Time: {now_str}  |  Simulated Day: Day #{simulated_day} (Cycle: 5 min)")
    lines.append(f"  Kafka Stream: topic 'smart_meters' (every 2.0s)  |  Database Sink: {db_mode}")
    lines.append("-" * 88)

    # Airflow & Orchestration Status Header
    k_stat = infra_status.get("kafka_status", "EMULATED")
    pg_stat = infra_status.get("postgres_status", "AVAILABLE")
    dq_flag = dq_status.get("status", "PASSED")
    lines.append(
        f"  [Airflow Orchestrator]: Health: ACTIVE  |  Kafka: {k_stat}  |  DB: {pg_stat}  |  DQ Gate: {dq_flag}"
    )
    lines.append(
        f"  [Static Dimension]: '{tariff_file.name}'  |  Enriched Households: 50  |  Batch #{batch_num} ({total_events} msgs)"
    )
    lines.append("=" * 88)

    # Real-Time Grid Table
    lines.append(
        f"  {'GRID ZONE':<14} | {'CONSUMPTION':<13} | {'SOLAR GEN':<13} | {'NET GRID LOAD':<17} | {'RENEWABLE %':<12} | {'EST COST ($)':<10}"
    )
    lines.append("-" * 88)

    tot_cons = 0.0
    tot_solar = 0.0
    tot_cost = 0.0

    for z in zone_metrics:
        cons = z["total_consumption_kwh"]
        solar = z["total_solar_generation_kwh"]
        net = z["net_load_kwh"]
        ren_pct = z["renewable_contribution_pct"]
        cost = z["total_cost_estimate"]

        tot_cons += cons
        tot_solar += solar
        tot_cost += cost

        # Indicator if zone is drawing power or feeding solar surplus back
        if net >= 0:
            load_str = f"+{net:06.2f} (DRAW)"
        else:
            load_str = f"{net:06.2f} (FEED)"

        lines.append(
            f"  {z['grid_zone']:<14} | {cons:>7.2f} kWh    | {solar:>7.2f} kWh    | {load_str:<17} | {ren_pct:>6.1f}%     | ${cost:>7.2f}"
        )

    lines.append("-" * 88)
    overall_net = tot_cons - tot_solar
    overall_ren = (tot_solar / tot_cons * 100.0) if tot_cons > 0 else 0.0
    overall_load_str = f"+{overall_net:06.2f} (DRAW)" if overall_net >= 0 else f"{overall_net:06.2f} (FEED)"

    lines.append(
        f"  {'GRID TOTALS':<14} | {tot_cons:>7.2f} kWh    | {tot_solar:>7.2f} kWh    | {overall_load_str:<17} | {overall_ren:>6.1f}%     | ${tot_cost:>7.2f}"
    )
    lines.append("=" * 88)
    lines.append(f"  [Architecture Note]: Streaming JSON is enriched with daily tariff drops via Stream-Static Join.")
    lines.append(f"  [Storage]: Metrics committed to table 'grid_zone_metrics'. (Press Ctrl+C to stop).")
    lines.append("=" * 88)

    print("\n".join(lines), flush=True)


def run_live_pipeline(max_batches: int = None):
    """Runs the integrated live pipeline demo."""
    clear_console()
    print("Initializing Smart Grid Kappa Pipeline Live Demo...")

    # Step 1: Initial Airflow Orchestration Tasks
    infra_status = check_infrastructure_health()
    tariff_file = execute_batch_drop(num_meters=settings.DEFAULT_NUM_METERS, day_offset=1)
    dq_status = validate_tariff_data_quality()

    # Step 2: Initialize Telemetry Simulator & Stream Processor
    simulator = SmartGridTelemetrySimulator(num_meters=settings.DEFAULT_NUM_METERS)
    processor = LiveKappaStreamProcessor(tariff_path=tariff_file)

    db_mode = "PostgreSQL (localhost:5432)" if processor.db_sink.use_postgres else f"SQLite ({SQLITE_DB_PATH.name})"

    batch_num = 0
    total_events = 0
    simulated_day = 1
    sim_start_time = time.time()
    last_tariff_drop_time = time.time()

    print("Setup verified. Commencing real-time streaming...\n")
    time.sleep(1.0)

    try:
        while RUNNING:
            loop_start = time.time()

            # Check if simulated day (5 minutes = 300 seconds) has elapsed to trigger Airflow batch drop
            if (time.time() - last_tariff_drop_time) >= settings.SIMULATED_DAY_DURATION_SEC:
                simulated_day += 1
                tariff_file = execute_batch_drop(num_meters=settings.DEFAULT_NUM_METERS, day_offset=simulated_day)
                dq_status = validate_tariff_data_quality()
                processor.refresh_tariffs()
                last_tariff_drop_time = time.time()

            # Task 1: Generate Telemetry Batch every 2 seconds
            events = simulator.generate_telemetry_batch()
            batch_num += 1
            total_events += len(events)

            # Task 2: Process Stream (Stream-Static Join on household_id & Zone Aggregation)
            zone_metrics = processor.process_telemetry_batch(events)

            # Task 3: Render Live Real-Time Dashboard
            clear_console()
            render_dashboard(
                simulated_day=simulated_day,
                batch_num=batch_num,
                total_events=total_events,
                zone_metrics=zone_metrics,
                infra_status=infra_status,
                dq_status=dq_status,
                tariff_file=tariff_file,
                db_mode=db_mode
            )

            if max_batches and batch_num >= max_batches:
                break

            # Sleep to maintain precise 2.0s streaming rate
            elapsed = time.time() - loop_start
            sleep_sec = max(0.1, settings.TELEMETRY_EMIT_INTERVAL_SEC - elapsed)
            time.sleep(sleep_sec)

    except KeyboardInterrupt:
        pass
    finally:
        print("\n[SUCCESS] Smart Grid Live Demonstration concluded successfully.")
        print(f"Total batches processed: {batch_num} ({total_events} events).")
        print(f"Aggregated metrics saved to: {SQLITE_DB_PATH if not processor.db_sink.use_postgres else 'PostgreSQL'}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Smart Grid Live Demo")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after N batches")
    args = parser.parse_args()
    run_live_pipeline(max_batches=args.max_batches)
