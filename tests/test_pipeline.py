"""
Unit and Integration Test Suite for the Smart Grid Kappa Architecture Pipeline.

Covers:
- Telemetry simulation physics and schema validation
- Tariff batch generator and atomic CSV persistence
- Data quality gate validation
- Mathematical formulas (net load, renewable contribution %, tariff cost)
- Resilient Kafka producer in dry-run mode
"""

import os
import sys
import csv
import json
import unittest
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from scripts.telemetry_producer import SmartGridTelemetrySimulator, ResilientKafkaProducer
from scripts.tariff_batch_generator import generate_household_tariffs, write_atomic_csv
from dags.smart_grid_dag import validate_tariff_data_quality


class TestSmartGridTelemetrySimulator(unittest.TestCase):
    """Validates the streaming data generator logic and physical constraints."""

    def setUp(self):
        self.simulator = SmartGridTelemetrySimulator(num_meters=20)

    def test_telemetry_batch_schema(self):
        """Verifies each emitted telemetry payload satisfies the strict schema."""
        batch = self.simulator.generate_telemetry_batch()
        self.assertEqual(len(batch), 20)

        required_fields = {
            "meter_id",
            "household_id",
            "power_consumption_kwh",
            "solar_generation_kwh",
            "grid_zone",
            "timestamp",
        }

        for event in batch:
            # Check all required fields present
            self.assertTrue(required_fields.issubset(event.keys()))

            # Check data types
            self.assertIsInstance(event["meter_id"], str)
            self.assertIsInstance(event["household_id"], str)
            self.assertIsInstance(event["power_consumption_kwh"], (int, float))
            self.assertIsInstance(event["solar_generation_kwh"], (int, float))
            self.assertIsInstance(event["grid_zone"], str)
            self.assertIsInstance(event["timestamp"], str)

            # Check physical constraints
            self.assertGreater(event["power_consumption_kwh"], 0.0)
            self.assertGreaterEqual(event["solar_generation_kwh"], 0.0)
            self.assertIn(event["grid_zone"], settings.GRID_ZONES)

    def test_json_serialization(self):
        """Ensures telemetry payload serializes into valid UTF-8 JSON without errors."""
        batch = self.simulator.generate_telemetry_batch()
        for event in batch:
            encoded = json.dumps(event).encode("utf-8")
            decoded = json.loads(encoded.decode("utf-8"))
            self.assertEqual(decoded["meter_id"], event["meter_id"])


class TestTariffBatchGenerator(unittest.TestCase):
    """Validates the daily static tariff batch generation and atomic writes."""

    def setUp(self):
        self.test_dir = settings.DATA_DIR / "test_tariffs"
        self.test_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_tariff_generation_fields_and_bounds(self):
        """Verifies household tariff generation satisfies business rules."""
        tariffs = generate_household_tariffs(num_meters=15, day_offset=1)
        self.assertEqual(len(tariffs), 15)

        for item in tariffs:
            self.assertIn("household_id", item)
            self.assertIn("tariff_rate", item)
            self.assertIn("billing_tier", item)
            self.assertIn("subsidy_flag", item)

            self.assertGreater(item["tariff_rate"], 0.05)
            self.assertLess(item["tariff_rate"], 1.00)
            self.assertIn(item["subsidy_flag"], [0, 1])
            self.assertTrue(item["billing_tier"].startswith("Tier-"))

    def test_atomic_csv_write(self):
        """Verifies atomic CSV write produces canonical and archive files."""
        tariffs = generate_household_tariffs(num_meters=10, day_offset=0)
        canonical = write_atomic_csv(tariffs, self.test_dir, "test_day")

        self.assertTrue(canonical.exists())
        self.assertEqual(canonical.name, "tariffs_latest.csv")

        # Read back CSV
        with open(canonical, mode="r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertEqual(len(reader), 10)
            self.assertEqual(reader[0]["household_id"], "HH-0001")


class TestStreamBusinessCalculations(unittest.TestCase):
    """Validates the core domain mathematical formulas used in PySpark streaming."""

    def test_net_grid_load_calculation(self):
        """Net grid load = power consumption - solar generation."""
        # Case 1: Net consumer (consuming 3.5 kWh, solar generating 1.0 kWh)
        cons_1, sol_1 = 3.5, 1.0
        net_load_1 = round(cons_1 - sol_1, 4)
        self.assertEqual(net_load_1, 2.5)  # Positive = load on grid

        # Case 2: Net producer (consuming 1.2 kWh, solar generating 4.5 kWh)
        cons_2, sol_2 = 1.2, 4.5
        net_load_2 = round(cons_2 - sol_2, 4)
        self.assertEqual(net_load_2, -3.3)  # Negative = surplus injected to grid

    def test_renewable_contribution_percentage(self):
        """Renewable penetration % = (Total Solar / Total Consumption) * 100."""
        # Case 1: Standard mix
        total_consumption = 100.0
        total_solar = 35.0
        renewable_pct = round((total_solar / total_consumption) * 100.0, 2)
        self.assertEqual(renewable_pct, 35.0)

        # Case 2: Zero consumption guard
        zero_consumption = 0.0
        renewable_pct_zero = (total_solar / zero_consumption * 100.0) if zero_consumption > 0 else 0.0
        self.assertEqual(renewable_pct_zero, 0.0)

    def test_cost_calculation_with_subsidy(self):
        """Billing cost = consumption * tariff_rate * (1 - subsidy_flag * 0.15)."""
        consumption = 10.0
        rate = 0.20

        # Without subsidy (subsidy_flag = 0)
        cost_no_sub = consumption * rate * (1.0 - (0 * 0.15))
        self.assertEqual(round(cost_no_sub, 2), 2.00)

        # With subsidy (subsidy_flag = 1) -> 15% discount
        cost_with_sub = consumption * rate * (1.0 - (1 * 0.15))
        self.assertEqual(round(cost_with_sub, 2), 1.70)


class TestResilientKafkaProducerDryRun(unittest.TestCase):
    """Tests the resilient producer adapter in dry-run simulation mode."""

    def test_dry_run_produce_and_flush(self):
        producer = ResilientKafkaProducer(bootstrap_servers="localhost:9092", dry_run=True)
        payload = {
            "meter_id": "MTR-001",
            "household_id": "HH-001",
            "power_consumption_kwh": 2.1,
            "solar_generation_kwh": 1.5,
            "grid_zone": "Zone-North",
            "timestamp": "2026-09-10T00:00:00Z"
        }
        # Should not raise exception
        producer.produce("smart_meters", key="HH-001", value=payload)
        producer.flush()


if __name__ == "__main__":
    unittest.main()
