"""
Daily Static Tariff Batch Data Generator.

Architectural Role in Kappa Architecture:
------------------------------------------
In Kappa Architecture, static reference dimensions (such as daily pricing models, 
tariffs, or billing tiers) are ingested periodically to enrich high-velocity streams. 
Instead of maintaining a separate batch analytics pipeline, the streaming engine 
joins this static dataset directly with the streaming events (Stream-Static Join).

Key Features:
- Simulates daily batch drops of tariff data (1 simulated day = 5 minutes).
- Emits CSV with fields: household_id, tariff_rate, billing_tier, subsidy_flag.
- Atomic File Drop Pattern: Writes to temporary file and atomically replaces destination, 
  preventing race conditions and partial reads by downstream streaming engines.
- Archives historical drops while updating the canonical 'tariffs_latest.csv'.
- Structured JSON logging.
"""

import os
import sys
import time
import shutil
import random
import argparse
from datetime import datetime, timezone, date
from pathlib import Path
from typing import List, Dict, Any

# Add parent directory to sys.path for relative imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings

logger = settings.get_logger("tariff_batch_generator")

BILLING_TIERS = {
    "Tier-1_Residential": {"min_rate": 0.12, "max_rate": 0.18, "weight": 0.70},
    "Tier-2_Commercial": {"min_rate": 0.20, "max_rate": 0.28, "weight": 0.20},
    "Tier-3_Industrial": {"min_rate": 0.30, "max_rate": 0.42, "weight": 0.10},
}


def generate_household_tariffs(num_meters: int, day_offset: int = 0) -> List[Dict[str, Any]]:
    """
    Generates realistic tariff rates per household for a given simulated day.
    Includes slight daily fluctuations simulating time-of-use market adjustments.
    """
    tariffs = []
    tiers = list(BILLING_TIERS.keys())
    weights = [BILLING_TIERS[t]["weight"] for t in tiers]

    for i in range(1, num_meters + 1):
        household_id = f"HH-{i:04d}"

        # Assign deterministic tier based on household index
        tier_index = (i % len(tiers))
        # Or weighted random:
        tier = random.choices(tiers, weights=weights)[0]
        cfg = BILLING_TIERS[tier]

        # Base rate with slight simulated daily fluctuation (+/- 5%)
        base_rate = round(random.uniform(cfg["min_rate"], cfg["max_rate"]), 4)
        daily_fluctuation = 1.0 + (math_fluctuation := ((hash(f"{household_id}-{day_offset}") % 10) - 5) * 0.01)
        tariff_rate = round(base_rate * daily_fluctuation, 4)

        # 30% of households qualify for green energy government subsidy
        subsidy_flag = 1 if (i % 3 == 0) else 0

        tariffs.append({
            "household_id": household_id,
            "tariff_rate": tariff_rate,
            "billing_tier": tier,
            "subsidy_flag": subsidy_flag,
        })

    return tariffs


def write_atomic_csv(data: List[Dict[str, Any]], target_dir: Path, day_label: str) -> Path:
    """
    Writes CSV using the Atomic Drop pattern:
    1. Writes to `.tmp_<filename>` in the target directory.
    2. Atomically renames to the final timestamped archive.
    3. Atomically updates the canonical `tariffs_latest.csv`.

    This guarantees downstream Spark stream-static joins never encounter partial writes.
    """
    import csv

    target_dir.mkdir(parents=True, exist_ok=True)

    archive_filename = f"tariffs_{day_label}.csv"
    archive_path = target_dir / archive_filename
    canonical_path = target_dir / "tariffs_latest.csv"

    temp_path = target_dir / f".tmp_{archive_filename}"
    fieldnames = ["household_id", "tariff_rate", "billing_tier", "subsidy_flag"]

    # Step 1: Write to temporary file using built-in csv writer
    with open(temp_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    # Step 2: Atomic rename to archive file
    if os.name == 'nt' and archive_path.exists():
        archive_path.unlink()
    temp_path.rename(archive_path)

    # Step 3: Update canonical tariffs_latest.csv atomically
    temp_canonical = target_dir / ".tmp_tariffs_latest.csv"
    shutil.copyfile(archive_path, temp_canonical)
    if os.name == 'nt' and canonical_path.exists():
        canonical_path.unlink()
    temp_canonical.rename(canonical_path)

    avg_tariff = sum(d["tariff_rate"] for d in data) / len(data) if data else 0.0
    subsidized_ratio = sum(d["subsidy_flag"] for d in data) / len(data) if data else 0.0

    logger.info(
        "Successfully dropped daily tariff CSV atomically",
        extra={"props": {
            "archive_file": str(archive_path),
            "canonical_file": str(canonical_path),
            "records_count": len(data),
            "avg_tariff": round(avg_tariff, 4),
            "subsidized_ratio": round(subsidized_ratio, 4)
        }}
    )

    return canonical_path


def execute_batch_drop(
    output_dir: Path = settings.RAW_TARIFF_DIR,
    num_meters: int = settings.DEFAULT_NUM_METERS,
    day_offset: int = 0
) -> Path:
    """Executes a single daily batch generation drop."""
    now_utc = datetime.now(timezone.utc)
    day_label = now_utc.strftime("%Y%m%d_%H%M%S")
    logger.info(
        f"Executing simulated daily tariff drop for offset={day_offset}, label={day_label}..."
    )
    tariffs = generate_household_tariffs(num_meters=num_meters, day_offset=day_offset)
    return write_atomic_csv(tariffs, output_dir, day_label)


def run_continuous_batch_simulation(
    output_dir: Path,
    num_meters: int,
    cycle_interval_sec: int = settings.SIMULATED_DAY_DURATION_SEC,
    max_cycles: int = None
):
    """
    Runs the batch generator in a continuous simulation loop:
    Drops a new daily tariff file every 5 minutes (300 seconds).
    """
    logger.info(
        "Starting continuous daily tariff drop simulation",
        extra={"props": {
            "output_dir": str(output_dir),
            "cycle_interval_sec": cycle_interval_sec,
            "meters": num_meters
        }}
    )
    cycle = 0
    try:
        while True:
            execute_batch_drop(output_dir=output_dir, num_meters=num_meters, day_offset=cycle)
            cycle += 1
            if max_cycles and cycle >= max_cycles:
                logger.info(f"Reached max cycles ({max_cycles}). Ending simulation.")
                break
            logger.info(f"Sleeping for {cycle_interval_sec}s until next simulated day drop...")
            time.sleep(cycle_interval_sec)
    except KeyboardInterrupt:
        logger.info("Batch generator stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate Daily Batch Tariff Drops")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=settings.RAW_TARIFF_DIR,
        help="Target directory for tariff CSV drops"
    )
    parser.add_argument(
        "--meters",
        type=int,
        default=settings.DEFAULT_NUM_METERS,
        help="Number of households to generate"
    )
    parser.add_argument(
        "--single-run",
        action="store_true",
        help="Run once (for Airflow task invocation) instead of infinite loop"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=settings.SIMULATED_DAY_DURATION_SEC,
        help="Simulated day duration in seconds (default: 300s = 5m)"
    )

    args = parser.parse_args()

    if args.single_run:
        execute_batch_drop(output_dir=args.output_dir, num_meters=args.meters)
    else:
        run_continuous_batch_simulation(
            output_dir=args.output_dir,
            num_meters=args.meters,
            cycle_interval_sec=args.interval
        )
