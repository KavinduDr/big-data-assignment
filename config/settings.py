"""
Settings and Configuration for the Smart Grid Energy Monitoring System.

Architectural Role:
Centralized single source of truth for runtime configurations, paths, 
connection parameters, and structured JSON logging formatting. Ensures 
environment-agnostic portability (local development, Docker containers, 
or production cluster).
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

# Base Directory Definitions
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_TARIFF_DIR = DATA_DIR / "raw_tariffs"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

# Ensure runtime directories exist
RAW_TARIFF_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# Apache Kafka Configuration
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TELEMETRY_TOPIC = os.getenv("KAFKA_TELEMETRY_TOPIC", "smart_meters")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "smart_meters_dlq")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "smart_grid_stream_group")
KAFKA_PRODUCER_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
KAFKA_RETRY_BACKOFF_MS = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "500"))

# --------------------------------------------------------------------------
# Simulation Constants
# --------------------------------------------------------------------------
# Streaming interval in seconds (every 2s telemetry emission)
TELEMETRY_EMIT_INTERVAL_SEC = float(os.getenv("TELEMETRY_EMIT_INTERVAL_SEC", "2.0"))

# Batch interval in seconds (1 simulated day = 5 minutes = 300 seconds)
SIMULATED_DAY_DURATION_SEC = int(os.getenv("SIMULATED_DAY_DURATION_SEC", "300"))

# Number of simulated smart meters / households
DEFAULT_NUM_METERS = int(os.getenv("DEFAULT_NUM_METERS", "50"))
GRID_ZONES = ["Zone-North", "Zone-South", "Zone-East", "Zone-West"]

# --------------------------------------------------------------------------
# Storage & PostgreSQL Configuration
# --------------------------------------------------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "smart_grid_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespassword")

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
POSTGRES_METRICS_TABLE = "grid_zone_metrics"
POSTGRES_HEALTH_TABLE = "pipeline_health_logs"

# --------------------------------------------------------------------------
# PySpark Structured Streaming Configuration
# --------------------------------------------------------------------------
SPARK_APP_NAME = "SmartGridKappaStreamProcessor"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY", "10 seconds")
WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
SLIDE_DURATION = os.getenv("SPARK_SLIDE_DURATION", "30 seconds")
TRIGGER_PROCESSING_TIME = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")

# PySpark Maven Package Dependencies for Kafka & Postgres JDBC
# Format: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3
SPARK_PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3"
)

# --------------------------------------------------------------------------
# Structured JSON Logging
# --------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    """
    Standardizes log records into structured JSON lines.
    Facilitates ingest into ELK, Loki, Datadog, or cloud monitoring systems.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        # Add custom attributes if passed in extra
        if hasattr(record, "props") and isinstance(record.props, dict):
            log_obj.update(record.props)
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Factory function for obtaining a preconfigured structured logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger
