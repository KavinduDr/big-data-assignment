"""
Live Stream Processing Engine (Pure-Python Kappa Architecture Implementation).

Purpose:
Provides a lightweight, zero-dependency streaming execution engine that implements 
the exact same Kappa architecture logic as 'stream_processor.py' (PySpark Structured Streaming) 
without requiring heavy external Java/Spark/Docker downloads.

Features:
1. Continuous stream ingestion of smart meter JSON telemetry.
2. Stream-Static Join with 'data/raw_tariffs/tariffs_latest.csv' on 'household_id'.
3. Real-time tumbling window aggregations by 'grid_zone':
   - Net Grid Load (Consumption - Solar Generation)
   - Renewable Contribution Percentage
   - Financial Billing Cost Estimates incorporating subsidies
4. Idempotent database sink to PostgreSQL (or zero-dependency SQLite fallback).
5. Full structured JSON logging.
"""

import os
import sys
import csv
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings

logger = settings.get_logger("live_stream_engine")

SQLITE_DB_PATH = settings.DATA_DIR / "smart_grid.db"


class DatabaseSink:
    """
    Database Sink supporting PostgreSQL with automatic zero-dependency SQLite fallback.
    Guarantees the live demo functions flawlessly regardless of external database state.
    """
    def __init__(self):
        self.use_postgres = False
        self.pg_conn = None
        self._init_sqlite()
        self._try_init_postgres()

    def _init_sqlite(self):
        """Initializes local SQLite schema identical to init_db.sql."""
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grid_zone_metrics (
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                grid_zone TEXT NOT NULL,
                total_consumption_kwh REAL NOT NULL,
                total_solar_generation_kwh REAL NOT NULL,
                net_load_kwh REAL NOT NULL,
                renewable_contribution_pct REAL NOT NULL,
                total_cost_estimate REAL NOT NULL,
                active_meters INTEGER NOT NULL,
                batch_id INTEGER,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (window_start, window_end, grid_zone)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_health_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                component TEXT NOT NULL,
                status TEXT NOT NULL,
                latency_ms REAL,
                details TEXT,
                checked_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        conn.close()

    def _try_init_postgres(self):
        """Attempts connection to PostgreSQL if psycopg2 is installed."""
        try:
            import psycopg2
            self.pg_conn = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                dbname=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                connect_timeout=3
            )
            self.use_postgres = True
            logger.info("DatabaseSink: Successfully connected to PostgreSQL!")
        except Exception:
            self.use_postgres = False
            logger.info(f"DatabaseSink: Using embedded SQL storage ({SQLITE_DB_PATH}) for live demo.")

    def write_zone_metrics(self, records: List[Dict[str, Any]], batch_id: int):
        """Persists windowed micro-batch metrics to database."""
        now_str = datetime.now(timezone.utc).isoformat()

        # 1. Write to SQLite (guaranteed zero-downtime)
        try:
            conn = sqlite3.connect(SQLITE_DB_PATH)
            cur = conn.cursor()
            for r in records:
                cur.execute("""
                    INSERT INTO grid_zone_metrics (
                        window_start, window_end, grid_zone, 
                        total_consumption_kwh, total_solar_generation_kwh, 
                        net_load_kwh, renewable_contribution_pct, 
                        total_cost_estimate, active_meters, batch_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(window_start, window_end, grid_zone) DO UPDATE SET
                        total_consumption_kwh = excluded.total_consumption_kwh,
                        total_solar_generation_kwh = excluded.total_solar_generation_kwh,
                        net_load_kwh = excluded.net_load_kwh,
                        renewable_contribution_pct = excluded.renewable_contribution_pct,
                        total_cost_estimate = excluded.total_cost_estimate,
                        active_meters = excluded.active_meters,
                        batch_id = excluded.batch_id,
                        updated_at = excluded.updated_at;
                """, (
                    r["window_start"], r["window_end"], r["grid_zone"],
                    r["total_consumption_kwh"], r["total_solar_generation_kwh"],
                    r["net_load_kwh"], r["renewable_contribution_pct"],
                    r["total_cost_estimate"], r["active_meters"], batch_id, now_str
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"SQLite write error: {e}")

        # 2. Write to PostgreSQL if available
        if self.use_postgres and self.pg_conn:
            try:
                cur = self.pg_conn.cursor()
                for r in records:
                    cur.execute("""
                        INSERT INTO grid_zone_metrics (
                            window_start, window_end, grid_zone, 
                            total_consumption_kwh, total_solar_generation_kwh, 
                            net_load_kwh, renewable_contribution_pct, 
                            total_cost_estimate, active_meters, batch_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (window_start, window_end, grid_zone) DO UPDATE SET
                            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                            total_solar_generation_kwh = EXCLUDED.total_solar_generation_kwh,
                            net_load_kwh = EXCLUDED.net_load_kwh,
                            renewable_contribution_pct = EXCLUDED.renewable_contribution_pct,
                            total_cost_estimate = EXCLUDED.total_cost_estimate,
                            active_meters = EXCLUDED.active_meters,
                            batch_id = EXCLUDED.batch_id,
                            updated_at = CURRENT_TIMESTAMP;
                    """, (
                        r["window_start"], r["window_end"], r["grid_zone"],
                        r["total_consumption_kwh"], r["total_solar_generation_kwh"],
                        r["net_load_kwh"], r["renewable_contribution_pct"],
                        r["total_cost_estimate"], r["active_meters"], batch_id
                    ))
                self.pg_conn.commit()
                cur.close()
            except Exception as e:
                logger.warning(f"PostgreSQL write failed (falling back to SQLite): {e}")
                self.use_postgres = False


class LiveKappaStreamProcessor:
    """
    Simulates real-time stream-static join and windowed micro-batch computation.
    """
    def __init__(self, tariff_path: Path = settings.RAW_TARIFF_DIR / "tariffs_latest.csv"):
        self.tariff_path = tariff_path
        self.db_sink = DatabaseSink()
        self.tariffs_cache: Dict[str, Dict[str, Any]] = {}
        self.batch_id = 0
        self.refresh_tariffs()

    def refresh_tariffs(self) -> int:
        """
        Reloads static tariff reference dataset (Stream-Static Join dimension).
        Called periodically to pick up new simulated daily drops automatically.
        """
        if not self.tariff_path.exists():
            return 0

        new_cache = {}
        try:
            with open(self.tariff_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    new_cache[row["household_id"]] = {
                        "tariff_rate": float(row["tariff_rate"]),
                        "billing_tier": row["billing_tier"],
                        "subsidy_flag": int(row["subsidy_flag"]),
                    }
            self.tariffs_cache = new_cache
            return len(self.tariffs_cache)
        except Exception as err:
            logger.warning(f"Could not refresh tariffs: {err}")
            return len(self.tariffs_cache)

    def process_telemetry_batch(self, telemetry_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Executes stream-static join and zone aggregations for a streaming batch.
        """
        self.batch_id += 1
        now = datetime.now(timezone.utc)
        window_start = now.strftime("%Y-%m-%d %H:%M:00")
        window_end = now.strftime("%Y-%m-%d %H:%M:30")

        # Grouping accumulator by zone
        zones_acc: Dict[str, Dict[str, Any]] = {}
        for zone in settings.GRID_ZONES:
            zones_acc[zone] = {
                "window_start": window_start,
                "window_end": window_end,
                "grid_zone": zone,
                "total_consumption_kwh": 0.0,
                "total_solar_generation_kwh": 0.0,
                "net_load_kwh": 0.0,
                "total_cost_estimate": 0.0,
                "active_meters": 0,
            }

        # 1. Stream-Static Join on household_id & Aggregation
        for event in telemetry_events:
            hh_id = event["household_id"]
            zone = event["grid_zone"]
            cons = float(event["power_consumption_kwh"])
            solar = float(event["solar_generation_kwh"])

            # Static Dimension Join
            tariff = self.tariffs_cache.get(hh_id, {
                "tariff_rate": 0.15,
                "billing_tier": "Standard",
                "subsidy_flag": 0
            })

            rate = tariff["tariff_rate"]
            subsidy = tariff["subsidy_flag"]
            # 15% discount if green subsidy applies
            cost = cons * rate * (1.0 - (subsidy * 0.15))

            if zone in zones_acc:
                acc = zones_acc[zone]
                acc["total_consumption_kwh"] += cons
                acc["total_solar_generation_kwh"] += solar
                acc["total_cost_estimate"] += cost
                acc["active_meters"] += 1

        # 2. Final metrics calculation (roundings, net load, renewable %)
        results = []
        for zone, acc in zones_acc.items():
            cons_sum = acc["total_consumption_kwh"]
            solar_sum = acc["total_solar_generation_kwh"]
            net_load = cons_sum - solar_sum

            # Renewable contribution %
            if cons_sum > 0:
                renewable_pct = round((solar_sum / cons_sum) * 100.0, 2)
            else:
                renewable_pct = 0.0

            metric = {
                "window_start": acc["window_start"],
                "window_end": acc["window_end"],
                "grid_zone": zone,
                "total_consumption_kwh": round(cons_sum, 4),
                "total_solar_generation_kwh": round(solar_sum, 4),
                "net_load_kwh": round(net_load, 4),
                "renewable_contribution_pct": renewable_pct,
                "total_cost_estimate": round(acc["total_cost_estimate"], 4),
                "active_meters": acc["active_meters"],
            }
            results.append(metric)

        # 3. Micro-batch sink to database
        self.db_sink.write_zone_metrics(results, batch_id=self.batch_id)
        return results
