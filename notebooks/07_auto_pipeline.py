# Databricks notebook source
# Module 8 — Automated Pipeline (Producer → Bronze → Silver → Gold → Log)
#
# This is the notebook to SCHEDULE as a Databricks Job (every 2 minutes).
# Fully automatic — no manual uploads.
#
# Flow:
#   1. Producer writes JSON to landing volume
#   2. Bronze ingests raw events
#   3. Silver cleans and validates
#   4. Gold computes windowed metrics
#   5. Run logged to pipeline_runs table
#   6. Dashboard auto-refreshes with new data

# COMMAND ----------

# MAGIC %md
# MAGIC # Smart Factory — Automated Pipeline
# MAGIC
# MAGIC ```
# MAGIC Producer → Landing → Bronze → Silver → Gold → Dashboard
# MAGIC ```
# MAGIC
# MAGIC Schedule this notebook as a Job. See `docs/JOB_SETUP.md`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("burst_seconds", str(PRODUCER_BURST_SECONDS), "Producer burst (seconds)")

import json
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import (
    avg, col, count, lower, max,
    sum as spark_sum, try_to_timestamp, when, window,
)

burst_seconds = int(dbutils.widgets.get("burst_seconds"))
run_id = str(uuid.uuid4())
run_started = time.monotonic()
run_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Run ID   : {run_id}")
print(f"Started  : {run_timestamp}")
print(f"Producer : {burst_seconds}s burst → {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate events (Producer → Landing)

# COMMAND ----------

# Inline producer (same logic as 02_producer.py — no %run to keep single-job reliability)
import random as _random

_STATUSES = ("running", "idle", "error")
_CORRUPTION_TYPES = (
    "null_machine_id", "high_temperature", "invalid_status",
    "bad_timestamp", "high_humidity", "high_vibration",
)


def _utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_machine_ids(count):
    width = max(2, len(str(count)))
    return [f"machine_{i:0{width}d}" for i in range(1, count + 1)]


def _generate_valid_event(machine_id):
    return {
        "machine_id": machine_id,
        "temperature": round(_random.uniform(45.0, 80.0), 1),
        "humidity": round(_random.uniform(30.0, 70.0), 1),
        "vibration": round(_random.uniform(0.1, 5.0), 1),
        "status": _random.choice(_STATUSES),
        "timestamp": _utc_now_iso(),
    }


def _corrupt_event(event):
    corrupted = dict(event)
    corruption = _random.choice(_CORRUPTION_TYPES)
    if corruption == "null_machine_id":
        corrupted["machine_id"] = None
    elif corruption == "high_temperature":
        corrupted["temperature"] = 999
    elif corruption == "invalid_status":
        corrupted["status"] = "melting"
    elif corruption == "bad_timestamp":
        corrupted["timestamp"] = "abc"
    elif corruption == "high_humidity":
        corrupted["humidity"] = 150
    elif corruption == "high_vibration":
        corrupted["vibration"] = 75
    return corrupted


def _event_filename(event):
    machine_part = event.get("machine_id") or "unknown"
    safe_ts = str(event.get("timestamp", "no_timestamp")).replace(":", "-")
    return f"{machine_part}_{safe_ts}_{uuid.uuid4().hex[:8]}.json"


def _write_event(event):
    path = f"{LANDING_PATH}/{_event_filename(event)}"
    dbutils.fs.put(path, json.dumps(event, indent=2) + "\n", overwrite=False)


def _count_landing_files():
    try:
        return len(dbutils.fs.ls(LANDING_PATH))
    except Exception:
        return 0


landing_before = _count_landing_files()
machine_ids = _build_machine_ids(MACHINE_COUNT)
batch_interval = 1.0 / EVENTS_PER_SECOND
producer_started = time.monotonic()
events_written = 0

while time.monotonic() - producer_started < burst_seconds:
    batch_start = time.monotonic()
    for machine_id in machine_ids:
        event = _generate_valid_event(machine_id)
        if CORRUPT_RATE > 0 and _random.random() < CORRUPT_RATE:
            event = _corrupt_event(event)
        _write_event(event)
        events_written += 1
    elapsed = time.monotonic() - batch_start
    sleep_for = batch_interval - elapsed
    if sleep_for > 0:
        time.sleep(sleep_for)

landing_after = _count_landing_files()
landing_files_added = landing_after - landing_before

print(f"Producer done — {events_written:,} events written, {landing_files_added:,} new landing files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Bronze (Landing → bronze_events)

# COMMAND ----------

event_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

def _table_count(path):
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0

bronze_before = _table_count(BRONZE_TABLE_PATH)

(
    spark.readStream.schema(event_schema).json(LANDING_PATH)
    .writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .start(BRONZE_TABLE_PATH)
    .awaitTermination()
)

bronze_after = _table_count(BRONZE_TABLE_PATH)
bronze_added = bronze_after - bronze_before
print(f"Bronze done — {bronze_after:,} total (+{bronze_added:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Silver (bronze_events → silver_events)

# COMMAND ----------

silver_before = _table_count(SILVER_TABLE_PATH)

silver_stream = (
    spark.readStream.format("delta").load(BRONZE_TABLE_PATH)
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("humidity", col("humidity").cast("double"))
    .withColumn("vibration", col("vibration").cast("double"))
    .withColumn("status", lower(col("status")))
    .withColumn("event_time", try_to_timestamp(col("timestamp")))
    .filter(
        col("machine_id").isNotNull()
        & col("temperature").between(-20, 150)
        & col("humidity").between(0, 100)
        & col("vibration").between(0, 50)
        & col("status").isin(VALID_STATUSES)
        & col("event_time").isNotNull()
    )
    .select("machine_id", "temperature", "humidity", "vibration", "status", "event_time")
)

(
    silver_stream.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start(SILVER_TABLE_PATH)
    .awaitTermination()
)

silver_after = _table_count(SILVER_TABLE_PATH)
silver_added = silver_after - silver_before
rejected_total = bronze_after - silver_after
quality_rate = round(100.0 * silver_after / bronze_after, 1) if bronze_after else 0.0

print(f"Silver done — {silver_after:,} total (+{silver_added:,}), rejected: {rejected_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Gold (silver_events → gold_machine_metrics)

# COMMAND ----------

gold_before = _table_count(GOLD_TABLE_PATH)

gold_metrics = (
    spark.readStream.format("delta").load(SILVER_TABLE_PATH)
    .withWatermark("event_time", "2 minutes")
    .groupBy(col("machine_id"), window(col("event_time"), "1 minute"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        avg("vibration").alias("avg_vibration"),
        count("*").alias("event_count"),
        spark_sum(when(col("status") == "error", 1).otherwise(0)).alias("error_count"),
    )
    .withColumn("is_overheating", col("max_temperature") > 85)
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
    .select(
        "machine_id", "window_start", "window_end",
        "avg_temperature", "max_temperature", "avg_vibration",
        "event_count", "error_count", "is_overheating",
    )
)

(
    gold_metrics.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .trigger(availableNow=True)
    .start(GOLD_TABLE_PATH)
    .awaitTermination()
)

gold_after = _table_count(GOLD_TABLE_PATH)
gold_added = gold_after - gold_before
print(f"Gold done — {gold_after:,} total (+{gold_added:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Log run to pipeline_runs (powers dashboard Tile 6)

# COMMAND ----------

duration_seconds = round(time.monotonic() - run_started, 1)

run_record = spark.createDataFrame([(
    run_id,
    run_timestamp,
    landing_files_added,
    bronze_added,
    silver_added,
    gold_added,
    bronze_after,
    silver_after,
    gold_after,
    rejected_total,
    quality_rate,
    duration_seconds,
    "success",
)], [
    "run_id", "run_timestamp", "landing_files_added",
    "bronze_rows_added", "silver_rows_added", "gold_windows_added",
    "bronze_rows_total", "silver_rows_total", "gold_windows_total",
    "rejected_rows_total", "quality_rate_pct", "duration_seconds", "status",
])

run_record.write.format("delta").mode("append").save(PIPELINE_RUNS_PATH)

display(run_record)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline complete

# COMMAND ----------

print()
print("=" * 60)
print("AUTOMATED PIPELINE RUN COMPLETE")
print("=" * 60)
print(f"  Run ID           : {run_id}")
print(f"  Duration         : {duration_seconds}s")
print(f"  Landing files    : +{landing_files_added:,}  (total {landing_after:,})")
print(f"  Bronze           : +{bronze_added:,}  (total {bronze_after:,})")
print(f"  Silver           : +{silver_added:,}  (total {silver_after:,})")
print(f"  Gold windows     : +{gold_added:,}  (total {gold_after:,})")
print(f"  Rejected         : {rejected_total:,}  (quality {quality_rate}%)")
print("=" * 60)
print("Dashboard will show updated data on next auto-refresh.")
