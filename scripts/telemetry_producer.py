"""
Smart Meter Telemetry Stream Generator (Apache Kafka Producer).

Architectural Role in Kappa Architecture:
------------------------------------------
In a Kappa architecture, the messaging system (Apache Kafka) serves as the 
immutable, append-only real-time log. It decouples high-frequency telemetry 
ingestion from downstream analytical stream processing. Every event is captured 
in sequence with at-least-once semantics, allowing real-time consumption by 
PySpark as well as historical reprocessing if analytical pipelines are replayed.

Key Features:
- Emits realistic smart meter JSON telemetry to Kafka topic 'smart_meters' every 2s.
- Partitions messages by 'household_id' to guarantee in-order delivery per household.
- Fallback support: Supports both 'confluent_kafka' and 'kafka-python'.
- Exponential backoff retry mechanism on network disconnections.
- Structured JSON logging for integration with enterprise observability stacks.
"""

import os
import sys
import time
import math
import json
import random
import signal
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directory to sys.path for relative imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings

logger = settings.get_logger("telemetry_producer")

# Global flag for graceful shutdown handling
RUNNING = True


def handle_shutdown_signal(signum, frame):
    """Handles SIGINT/SIGTERM for zero-data-loss graceful producer shutdown."""
    global RUNNING
    logger.info("Shutdown signal received. Initiating graceful producer drain...")
    RUNNING = False


# Register OS signals
signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)


# --------------------------------------------------------------------------
# Abstract Producer Adapter (Confluent-Kafka & Kafka-Python Dual Support)
# --------------------------------------------------------------------------
class ResilientKafkaProducer:
    """
    Unified Kafka Producer Wrapper supporting confluent_kafka, kafka-python, 
    and a dry-run mock mode for validation without active Kafka cluster.
    Implements connection retry with exponential backoff and message delivery callbacks.
    """
    def __init__(self, bootstrap_servers: str, max_retries: int = 5, backoff_ms: int = 1000, dry_run: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.backoff_ms = backoff_ms
        self.dry_run = dry_run
        self.driver_type = "dry_run" if dry_run else None
        self.producer = None
        if not self.dry_run:
            self._initialize_client()
        else:
            logger.info("Initialized ResilientKafkaProducer in DRY-RUN mode (no broker required).")

    def _initialize_client(self):
        """Attempts to initialize producer using available library with retries."""
        for attempt in range(1, self.max_retries + 1):
            # Attempt 1: confluent_kafka (C-based, high throughput)
            try:
                import confluent_kafka
                conf = {
                    'bootstrap.servers': self.bootstrap_servers,
                    'client.id': 'smart-grid-telemetry-producer',
                    'acks': 'all',  # Guarantee durability
                    'retries': 3,
                    'retry.backoff.ms': 250,
                    'compression.type': 'snappy',
                }
                self.producer = confluent_kafka.Producer(conf)
                self.driver_type = "confluent_kafka"
                logger.info(
                    "Connected to Kafka via confluent_kafka",
                    extra={"props": {"servers": self.bootstrap_servers, "attempt": attempt}}
                )
                return
            except ImportError:
                pass
            except Exception as e:
                logger.warning(
                    f"confluent-kafka connection failed on attempt {attempt}: {e}"
                )

            # Attempt 2: kafka-python (Pure Python fallback)
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8'),
                    acks='all',
                    retries=3,
                    retry_backoff_ms=250,
                )
                self.driver_type = "kafka-python"
                logger.info(
                    "Connected to Kafka via kafka-python",
                    extra={"props": {"servers": self.bootstrap_servers, "attempt": attempt}}
                )
                return
            except ImportError:
                pass
            except Exception as e:
                logger.warning(
                    f"kafka-python connection failed on attempt {attempt}: {e}"
                )

            # If both failed, backoff and retry
            wait_time = (self.backoff_ms * (2 ** (attempt - 1))) / 1000.0
            logger.warning(
                f"Kafka broker unavailable at {self.bootstrap_servers}. Retrying in {wait_time:.1f}s..."
            )
            time.sleep(wait_time)

        raise ConnectionError(
            f"Failed to connect to Kafka at {self.bootstrap_servers} after {self.max_retries} attempts. "
            "Please ensure either confluent-kafka or kafka-python is installed and broker is running, "
            "or use --dry-run for local test simulation."
        )

    def produce(self, topic: str, key: str, value: Dict[str, Any]):
        """Produces a JSON event to Kafka with error handling."""
        if self.dry_run:
            # Simulate serialization verification
            _ = json.dumps(value).encode('utf-8')
            return

        if self.driver_type == "confluent_kafka":
            try:
                payload = json.dumps(value).encode('utf-8')
                self.producer.produce(
                    topic=topic,
                    key=str(key).encode('utf-8'),
                    value=payload,
                    callback=self._confluent_delivery_report
                )
                # Poll to serve delivery callbacks
                self.producer.poll(0)
            except Exception as err:
                logger.error(
                    f"Failed to produce message via confluent_kafka: {err}",
                    extra={"props": {"topic": topic, "key": key}}
                )
                raise
        elif self.driver_type == "kafka-python":
            try:
                future = self.producer.send(topic=topic, key=key, value=value)
                # Ensure exceptions are raised if synchronous dispatch fails
                future.add_errback(lambda err: logger.error(f"Async send error: {err}"))
            except Exception as err:
                logger.error(
                    f"Failed to produce message via kafka-python: {err}",
                    extra={"props": {"topic": topic, "key": key}}
                )
                raise

    @staticmethod
    def _confluent_delivery_report(err, msg):
        """Delivery callback for confluent_kafka."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def flush(self, timeout: float = 5.0):
        """Flushes buffered records before termination."""
        if self.producer:
            logger.info("Flushing Kafka producer buffer...")
            if self.driver_type == "confluent_kafka":
                self.producer.flush(timeout=timeout)
            elif self.driver_type == "kafka-python":
                self.producer.flush(timeout=timeout)


# --------------------------------------------------------------------------
# Physics & Telemetry Simulator
# --------------------------------------------------------------------------
class SmartGridTelemetrySimulator:
    """
    Simulates realistic smart meter telemetry reflecting diurnal sunlight 
    and residential/commercial consumption patterns.
    """
    def __init__(self, num_meters: int = settings.DEFAULT_NUM_METERS):
        self.num_meters = num_meters
        self.meters = self._generate_meter_registry()
        self.sim_start_time = time.time()

    def _generate_meter_registry(self):
        """Pre-allocates meter metadata across grid zones."""
        registry = []
        for i in range(1, self.num_meters + 1):
            zone = settings.GRID_ZONES[(i - 1) % len(settings.GRID_ZONES)]
            registry.append({
                "meter_id": f"MTR-{zone[:6].upper()}-{i:04d}",
                "household_id": f"HH-{i:04d}",
                "grid_zone": zone,
                # Solar capacity varies: 70% of households have rooftop solar installations
                "has_solar": (i % 10) < 7,
                "solar_capacity_kw": round(random.uniform(3.0, 8.5), 2) if (i % 10) < 7 else 0.0,
                # Baseline load factor
                "base_load_kw": round(random.uniform(0.8, 3.2), 2),
            })
        return registry

    def generate_telemetry_batch(self) -> list[Dict[str, Any]]:
        """
        Generates one telemetry reading per meter for the current timestamp.
        Simulates diurnal solar curves and load fluctuations.
        """
        now = datetime.now(timezone.utc)
        iso_timestamp = now.isoformat()

        # Simulate diurnal cycle: calculate solar zenith angle based on simulated time
        # In a compressed simulation, we cycle every 5 minutes (300s) = 1 simulated day
        elapsed_sec = (time.time() - self.sim_start_time) % settings.SIMULATED_DAY_DURATION_SEC
        simulated_hour = (elapsed_sec / settings.SIMULATED_DAY_DURATION_SEC) * 24.0

        # Daylight curve: daylight exists between 06:00 and 18:00, peaking at 12:00
        if 6.0 <= simulated_hour <= 18.0:
            # Normalized bell-shaped solar intensity curve
            solar_intensity = math.sin((simulated_hour - 6.0) / 12.0 * math.pi)
            # Add slight cloud variance (noise +/- 10%)
            solar_intensity *= random.uniform(0.9, 1.0)
        else:
            solar_intensity = 0.0

        batch = []
        for meter in self.meters:
            # 1. Power Consumption Simulation
            # Base load + diurnal evening peak (between 18:00 and 22:00) + random appliance spikes
            evening_peak = 1.8 if (18.0 <= simulated_hour <= 22.0) else 1.0
            appliance_spike = random.choice([0.0, 0.0, 0.0, 1.2, 2.5])  # Intermittent heater/AC/kettle
            consumption = (meter["base_load_kw"] * evening_peak) + appliance_spike + random.uniform(-0.15, 0.15)
            # 2-second energy equivalent: kWh = kW * (2 / 3600)
            # For realistic observable values in streaming windows, we report instant equivalent kWh
            power_consumption_kwh = max(0.05, round(consumption, 3))

            # 2. Solar Generation Simulation
            solar_generation_kwh = 0.0
            if meter["has_solar"] and solar_intensity > 0:
                gen = meter["solar_capacity_kw"] * solar_intensity
                solar_generation_kwh = max(0.0, round(gen, 3))

            telemetry_event = {
                "meter_id": meter["meter_id"],
                "household_id": meter["household_id"],
                "power_consumption_kwh": power_consumption_kwh,
                "solar_generation_kwh": solar_generation_kwh,
                "grid_zone": meter["grid_zone"],
                "timestamp": iso_timestamp,
            }
            batch.append(telemetry_event)

        return batch


# --------------------------------------------------------------------------
# Main Execution Loop
# --------------------------------------------------------------------------
def run_telemetry_stream(
    bootstrap_servers: str,
    topic: str,
    emit_interval: float = 2.0,
    num_meters: int = 50,
    max_batches: Optional[int] = None,
    dry_run: bool = False,
):
    """
    Continuous streaming loop emitting telemetry every `emit_interval` seconds.
    """
    logger.info(
        "Starting Smart Grid Telemetry Producer",
        extra={"props": {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "interval_sec": emit_interval,
            "meters": num_meters,
            "dry_run": dry_run
        }}
    )

    producer = ResilientKafkaProducer(bootstrap_servers=bootstrap_servers, dry_run=dry_run)
    simulator = SmartGridTelemetrySimulator(num_meters=num_meters)

    batch_count = 0
    total_records = 0

    try:
        while RUNNING:
            start_tick = time.time()
            telemetry_events = simulator.generate_telemetry_batch()

            for event in telemetry_events:
                # Partition by household_id to ensure FIFO ordering per household partition in Kafka
                producer.produce(
                    topic=topic,
                    key=event["household_id"],
                    value=event
                )

            batch_count += 1
            total_records += len(telemetry_events)

            logger.info(
                f"Emitted telemetry batch #{batch_count} ({len(telemetry_events)} events)",
                extra={"props": {
                    "batch_id": batch_count,
                    "records": len(telemetry_events),
                    "total_emitted": total_records,
                    "sample_event": telemetry_events[0]
                }}
            )

            if max_batches and batch_count >= max_batches:
                logger.info(f"Reached configured max batches ({max_batches}). Terminating producer.")
                break

            # Maintain strict cadence
            elapsed = time.time() - start_tick
            sleep_time = max(0.0, emit_interval - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught in producer loop.")
    except Exception as exc:
        logger.error(f"Fatal error in telemetry producer: {exc}", exc_info=True)
        raise
    finally:
        producer.flush(timeout=5.0)
        logger.info(
            "Telemetry producer shutdown complete.",
            extra={"props": {"total_batches": batch_count, "total_records": total_records}}
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Grid Kafka Telemetry Producer")
    parser.add_argument("--bootstrap-servers", default=settings.KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=settings.KAFKA_TELEMETRY_TOPIC, help="Kafka destination topic")
    parser.add_argument("--interval", type=float, default=settings.TELEMETRY_EMIT_INTERVAL_SEC, help="Emission interval in seconds")
    parser.add_argument("--meters", type=int, default=settings.DEFAULT_NUM_METERS, help="Number of simulated meters")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after N batches (useful for automated testing)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate message generation and serialization without connecting to Kafka broker")

    args = parser.parse_args()
    run_telemetry_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        emit_interval=args.interval,
        num_meters=args.meters,
        max_batches=args.max_batches,
        dry_run=args.dry_run,
    )
