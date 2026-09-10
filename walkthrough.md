# Walkthrough: Smart Grid Energy Monitoring Kappa Architecture Pipeline

We have designed, built, and verified an end-to-end Kappa architecture data pipeline simulating a Smart Grid Energy Monitoring system.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        P1["telemetry_producer.py<br/>(Every 2s JSON Stream)"] -->|Topic: smart_meters| KFK[("Apache Kafka<br/>Broker (KRaft)")]
        P2["tariff_batch_generator.py<br/>(1 Sim Day = 5 Minutes)"] -->|Atomic File Swap| CSV[("Static Reference Drops<br/>data/raw_tariffs/")]
    end

    subgraph Processing["2. Stream Processing Layer (PySpark)"]
        KFK -->|Kafka Consumer Stream| SP["stream_processor.py<br/>(PySpark Structured Streaming)"]
        CSV -->|Static Dimension Source| SP
        SP -->|Stream-Static Join on household_id| JOIN["Enriched Stream"]
        JOIN -->|Watermark: 10s | Window: 1m| AGG["Zone Window Aggregation<br/>- Net Grid Load<br/>- Renewable Contribution %<br/>- Financial Cost Estimates"]
    end

    subgraph Storage["3. Serving & Storage Layer"]
        AGG -->|foreachBatch JDBC Sink| PG[("PostgreSQL<br/>grid_zone_metrics")]
    end

    subgraph Orchestration["4. Orchestration & Monitoring"]
        AF["Apache Airflow DAG<br/>Schedule: */5 * * * *"] -->|Scheduled Drop Trigger| P2
        AF -->|Data Quality Gate| CSV
        AF -->|Broker Connectivity Probe| KFK
        AF -->|Freshness & Lag Monitor| PG
    end
```

---

## 2. Changes Made & Files Created

| Component | File | Description |
| :--- | :--- | :--- |
| **Config & Logging** | [`config/settings.py`](file:///e:/Github/big-data-assignment/config/settings.py) | Centralized environment configurations and structured JSON logger (`JsonFormatter`). |
| **Task 1 (Streaming)** | [`scripts/telemetry_producer.py`](file:///e:/Github/big-data-assignment/scripts/telemetry_producer.py) | Emits smart meter JSON telemetry to Kafka topic `smart_meters` every 2s with physics diurnal curve simulation, dual Kafka adapter support, exponential retries, and `--dry-run` mode. |
| **Task 1 (Batch)** | [`scripts/tariff_batch_generator.py`](file:///e:/Github/big-data-assignment/scripts/tariff_batch_generator.py) | Generates simulated daily tariff CSV drops (5 mins = 1 day) using an **Atomic File Swap** pattern to prevent stream read corruption. |
| **Task 2 (PySpark)** | [`streaming/stream_processor.py`](file:///e:/Github/big-data-assignment/streaming/stream_processor.py) | PySpark Structured Streaming application reading from Kafka, performing a **Stream-Static Join** with the tariff CSV, aggregating grid load and renewable penetration by zone with watermarking, and sinking to PostgreSQL via JDBC. |
| **Task 3 (Airflow)** | [`dags/smart_grid_dag.py`](file:///e:/Github/big-data-assignment/dags/smart_grid_dag.py) | Airflow DAG scheduled every 5 minutes (`*/5 * * * *`) that triggers tariff generation, runs a Data Quality Gate, checks broker/DB connectivity, and logs pipeline health. |
| **Database DDL** | [`docker/init_db.sql`](file:///e:/Github/big-data-assignment/docker/init_db.sql) | DDL schema for PostgreSQL: `grid_zone_metrics`, `pipeline_health_logs`, and indexes. |
| **Docker Stack** | [`docker/docker-compose.yml`](file:///e:/Github/big-data-assignment/docker/docker-compose.yml) | Multi-service environment containing Kafka (KRaft), Kafka UI, PostgreSQL 16, and Apache Airflow. |
| **Test Suite** | [`tests/test_pipeline.py`](file:///e:/Github/big-data-assignment/tests/test_pipeline.py) | 8 unit and integration tests covering simulation physics, schema conformance, atomic writes, and domain math. |
| **Chapter 3 Lab** | [`avro_order_lab/`](file:///e:/Github/big-data-assignment/avro_order_lab/) | Implementation of the assignment PDF brief: `order.avsc` and `order_producer_consumer.py` with Avro serialization, running average, retries, and DLQ. |
| **Documentation** | [`README.md`](file:///e:/Github/big-data-assignment/README.md) | Full architectural explanation, Kappa vs. Lambda trade-offs, and quickstart run instructions. |

---

## 3. Verification Results

### 1. Automated Test Suite
Ran `python -m unittest discover -s tests -p "test_*.py" -v`:
```text
test_telemetry_batch_schema (tests.test_pipeline.TestSmartGridTelemetrySimulator) ... ok
test_json_serialization (tests.test_pipeline.TestSmartGridTelemetrySimulator) ... ok
test_tariff_generation_fields_and_bounds (tests.test_pipeline.TestTariffBatchGenerator) ... ok
test_atomic_csv_write (tests.test_pipeline.TestTariffBatchGenerator) ... ok
test_cost_calculation_with_subsidy (tests.test_pipeline.TestStreamBusinessCalculations) ... ok
test_net_grid_load_calculation (tests.test_pipeline.TestStreamBusinessCalculations) ... ok
test_renewable_contribution_percentage (tests.test_pipeline.TestStreamBusinessCalculations) ... ok
test_dry_run_produce_and_flush (tests.test_pipeline.TestResilientKafkaProducerDryRun) ... ok

----------------------------------------------------------------------
Ran 8 tests in 0.011s

OK
```

### 2. Task 1: Telemetry Producer Execution
Tested with `--dry-run --max-batches 2`:
```json
{"timestamp": "2026-09-10T03:52:04.676430+00:00", "level": "INFO", "logger": "telemetry_producer", "message": "Emitted telemetry batch #1 (50 events)", "batch_id": 1, "records": 50, "total_emitted": 50, "sample_event": {"meter_id": "MTR-ZONE-N-0001", "household_id": "HH-0001", "power_consumption_kwh": 1.329, "solar_generation_kwh": 0.0, "grid_zone": "Zone-North", "timestamp": "2026-09-10T03:52:04.675924+00:00"}}
```

### 3. Task 1: Tariff Batch Generator Execution
Tested with `--single-run`:
- Successfully generated canonical [`data/raw_tariffs/tariffs_latest.csv`](file:///e:/Github/big-data-assignment/data/raw_tariffs/tariffs_latest.csv) with fields `household_id,tariff_rate,billing_tier,subsidy_flag`.

### 4. Task 3: Airflow Pipeline Standalone Run
Tested execution of pipeline tasks directly via `python dags/smart_grid_dag.py`:
- Infrastructure connectivity check: completed.
- Daily tariff generation: committed new drop.
- Data Quality Gate: **PASSED** (validated 50 records).
- Freshness & health check: completed and logged.

### 5. Assignment Chapter 3 Companion Lab Run
Tested `python avro_order_lab/order_producer_consumer.py`:
- Successfully demonstrated Avro transaction stream, running average calculation, transient retry handling, and Dead Letter Queue (DLQ) routing for poison pills.
