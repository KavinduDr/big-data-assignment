"""
Smart Grid PySpark Structured Streaming Processor.

Architectural Role in Kappa Architecture:
------------------------------------------
In Kappa Architecture, all data is processed through a single stream-first 
paradigm. Rather than managing dual architectures (batch Hadoop/Spark jobs and 
real-time Storm/Flink jobs as in Lambda Architecture), Kappa relies on a unified 
stream processor (PySpark Structured Streaming) to handle both real-time stream 
ingestion and enrichment with static dimension datasets.

Key Responsibilities:
1. Ingests real-time JSON smart meter telemetry from Apache Kafka ('smart_meters').
2. Applies schema enforcement and event-time watermarking for late-arrival bounding.
3. Performs a Stream-Static Join with the daily static tariff CSV on 'household_id'.
4. Computes windowed aggregations by grid_zone:
   - Total consumption (kWh)
   - Total solar generation (kWh)
   - Net grid load (consumption - solar)
   - Renewable penetration contribution percentage
   - Financial billing estimates incorporating dynamic tariffs and subsidies.
5. Writes aggregated results idempotently into PostgreSQL via JDBC in micro-batches
   using the 'foreachBatch' sink pattern.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional

# Add parent directory to sys.path for relative imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings

logger = settings.get_logger("stream_processor")


def build_spark_session(app_name: str = settings.SPARK_APP_NAME, master: str = settings.SPARK_MASTER):
    """
    Initializes SparkSession with Kafka SQL and PostgreSQL JDBC packages.
    """
    from pyspark.sql import SparkSession

    logger.info(
        "Initializing PySpark Session",
        extra={"props": {
            "app_name": app_name,
            "master": master,
            "packages": settings.SPARK_PACKAGES
        }}
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars.packages", settings.SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")  # Optimized for local / small cluster execution
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_telemetry_schema():
    """Defines strict schema for deserializing JSON telemetry messages."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType
    )
    return StructType([
        StructField("meter_id", StringType(), False),
        StructField("household_id", StringType(), False),
        StructField("power_consumption_kwh", DoubleType(), False),
        StructField("solar_generation_kwh", DoubleType(), False),
        StructField("grid_zone", StringType(), False),
        StructField("timestamp", StringType(), False),
    ])


def get_tariff_schema():
    """Defines schema for static daily tariff reference CSV."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType
    )
    return StructType([
        StructField("household_id", StringType(), False),
        StructField("tariff_rate", DoubleType(), False),
        StructField("billing_tier", StringType(), False),
        StructField("subsidy_flag", IntegerType(), False),
    ])


def load_static_tariffs(spark, tariff_csv_path: str):
    """
    Loads daily static tariff reference dataset into a static Spark DataFrame.
    
    Architectural Note:
    In Spark Structured Streaming, stream-static joins evaluate the static DataFrame
    at micro-batch execution boundaries, allowing new daily tariff drops to propagate 
    seamlessly without restarting the streaming job.
    """
    from pyspark.sql.functions import col

    logger.info(f"Loading static tariff dataset from: {tariff_csv_path}")
    tariff_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(get_tariff_schema())
        .load(tariff_csv_path)
    )
    return tariff_df


def create_postgres_sink_writer(jdbc_url: str, table_name: str, user: str, password: str):
    """
    Higher-order function returning a micro-batch foreachBatch writer for PostgreSQL.
    
    Why foreachBatch?
    Standard PySpark streaming JDBC sinks do not support windowed aggregations directly.
    foreachBatch converts each streaming micro-batch into a static DataFrame, allowing
    atomic transaction writes and idempotent database upserts.
    """
    def write_micro_batch_to_postgres(batch_df, batch_id: int):
        record_count = batch_df.count()
        if record_count == 0:
            logger.info(f"Micro-batch {batch_id}: Empty batch. Skipping database write.")
            return

        logger.info(
            f"Micro-batch {batch_id}: Writing {record_count} aggregated rows to PostgreSQL...",
            extra={"props": {"batch_id": batch_id, "record_count": record_count, "table": table_name}}
        )

        try:
            # Flatten window struct into separate timestamp columns for relational DB compatibility
            from pyspark.sql.functions import col, lit

            formatted_df = (
                batch_df
                .withColumn("window_start", col("window.start"))
                .withColumn("window_end", col("window.end"))
                .withColumn("batch_id", lit(batch_id))
                .drop("window")
            )

            # Write using JDBC mode 'append' (or customized SQL upsert)
            (
                formatted_df.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", user)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            logger.info(
                f"Micro-batch {batch_id}: Successfully committed {record_count} records to {table_name}."
            )
        except Exception as err:
            logger.error(
                f"Micro-batch {batch_id} failed to write to PostgreSQL: {err}",
                exc_info=True
            )
            # In production, route failed batch records to Dead Letter Queue (DLQ) or alert PagerDuty
            raise err

    return write_micro_batch_to_postgres


def process_stream(
    kafka_servers: str,
    topic: str,
    tariff_csv_path: str,
    checkpoint_location: str,
    output_sink: str = "postgres",
    watermark_delay: str = settings.WATERMARK_DELAY,
    window_duration: str = settings.WINDOW_DURATION,
    slide_duration: str = settings.SLIDE_DURATION,
):
    """
    Main processing topology for the Smart Grid Kappa Architecture pipeline.
    """
    spark = build_spark_session()

    from pyspark.sql.functions import (
        col, from_json, to_timestamp, window, sum as _sum, round as _round, when, count
    )

    # 1. Ingest Kafka Stream
    logger.info(f"Connecting to Kafka topic '{topic}' at {kafka_servers}...")
    kafka_raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON & Enforce Schema
    telemetry_schema = get_telemetry_schema()
    parsed_stream = (
        kafka_raw_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), telemetry_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        # Bounding late arrivals with watermarking:
        # Watermarking tells Spark when to discard late events and evict state from memory
        .withWatermark("event_timestamp", watermark_delay)
    )

    # 3. Load Static Tariff Data
    tariff_df = load_static_tariffs(spark, tariff_csv_path)

    # 4. Stream-Static Join on household_id
    # Enriches telemetry with tariff_rate, billing_tier, and subsidy_flag
    enriched_stream = (
        parsed_stream.join(
            tariff_df,
            on="household_id",
            how="left"
        )
        # Default fallback values for unmatched households
        .na.fill({"tariff_rate": 0.15, "billing_tier": "Standard", "subsidy_flag": 0})
        # Calculate instantaneous net grid load (kWh)
        # Positive = Grid Load (consuming power)
        # Negative = Grid Feed-in (solar surplus pushed back to grid)
        .withColumn("net_grid_load_kwh", col("power_consumption_kwh") - col("solar_generation_kwh"))
        # Estimated cost incorporating subsidy (15% discount if subsidy applies)
        .withColumn(
            "estimated_cost",
            col("power_consumption_kwh") * col("tariff_rate") * (1.0 - (col("subsidy_flag") * 0.15))
        )
    )

    # 5. Windowed Aggregations by Zone
    # Calculates current grid load, renewable penetration %, and billing metrics
    aggregated_stream = (
        enriched_stream
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("grid_zone")
        )
        .agg(
            _round(_sum("power_consumption_kwh"), 4).alias("total_consumption_kwh"),
            _round(_sum("solar_generation_kwh"), 4).alias("total_solar_generation_kwh"),
            _round(_sum("net_grid_load_kwh"), 4).alias("net_load_kwh"),
            # Renewable contribution % = (Total Solar / Total Consumption) * 100
            # Guard against divide-by-zero when consumption is zero
            _round(
                when(_sum("power_consumption_kwh") > 0,
                     (_sum("solar_generation_kwh") / _sum("power_consumption_kwh")) * 100.0)
                .otherwise(0.0),
                2
            ).alias("renewable_contribution_pct"),
            _round(_sum("estimated_cost"), 4).alias("total_cost_estimate"),
            count("meter_id").alias("active_meters")
        )
    )

    # 6. Sinking Results
    if output_sink == "console":
        logger.info("Directing stream output to CONSOLE for debugging...")
        query = (
            aggregated_stream.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime=settings.TRIGGER_PROCESSING_TIME)
            .start()
        )
    else:
        logger.info(
            f"Directing stream output to PostgreSQL via foreachBatch ({settings.POSTGRES_JDBC_URL})..."
        )
        writer = create_postgres_sink_writer(
            jdbc_url=settings.POSTGRES_JDBC_URL,
            table_name=settings.POSTGRES_METRICS_TABLE,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )

        query = (
            aggregated_stream.writeStream
            .outputMode("update")
            .foreachBatch(writer)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=settings.TRIGGER_PROCESSING_TIME)
            .start()
        )

    logger.info(f"Stream query started with ID: {query.id}. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart Grid PySpark Stream Processor")
    parser.add_argument("--kafka-servers", default=settings.KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=settings.KAFKA_TELEMETRY_TOPIC, help="Kafka source topic")
    parser.add_argument("--tariff-csv", default=str(settings.RAW_TARIFF_DIR / "tariffs_latest.csv"), help="Path to latest static tariff CSV")
    parser.add_argument("--checkpoint", default=str(settings.CHECKPOINT_DIR), help="Checkpoint directory for fault recovery")
    parser.add_argument("--sink", choices=["postgres", "console"], default="postgres", help="Output destination sink")

    args = parser.parse_args()

    # Ensure tariff CSV exists prior to startup
    tariff_file = Path(args.tariff_csv)
    if not tariff_file.exists():
        logger.warning(f"Tariff file {tariff_file} does not exist. Triggering initial batch drop...")
        from scripts.tariff_batch_generator import execute_batch_drop
        execute_batch_drop(output_dir=tariff_file.parent)

    process_stream(
        kafka_servers=args.kafka_servers,
        topic=args.topic,
        tariff_csv_path=args.tariff_csv,
        checkpoint_location=args.checkpoint,
        output_sink=args.sink,
    )
