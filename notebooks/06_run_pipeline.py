# Databricks notebook source
# Module 7 — Run Full Pipeline (Landing → Bronze → Silver → Gold)
#
# Run this notebook after new JSON files are in the landing volume.
# It executes all three streaming layers in order and prints a health summary.
#
# Dashboard: docs/dashboard_queries.sql + docs/DASHBOARD_SETUP.md

# COMMAND ----------

# MAGIC %md
# MAGIC # Smart Factory — Full Medallion Pipeline
# MAGIC
# MAGIC ```
# MAGIC Landing (JSON) → Bronze (raw) → Silver (clean) → Gold (metrics) → Dashboard
# MAGIC ```
# MAGIC
# MAGIC **Before running:** upload or produce new JSON files in the landing volume.

# COMMAND ----------

LANDING_PATH = "/Volumes/workspace/default/smart_factory/landing"
BRONZE_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/bronze_events"
SILVER_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/silver_events"
GOLD_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/gold_machine_metrics"
CHECKPOINT_BRONZE = "/Volumes/workspace/default/smart_factory/checkpoints/bronze"
CHECKPOINT_SILVER = "/Volumes/workspace/default/smart_factory/checkpoints/silver"
CHECKPOINT_GOLD = "/Volumes/workspace/default/smart_factory/checkpoints/gold"

VALID_STATUSES = ["running", "idle", "error"]

print("Pipeline paths configured.")
print("  Landing :", LANDING_PATH)
print("  Bronze  :", BRONZE_TABLE_PATH)
print("  Silver  :", SILVER_TABLE_PATH)
print("  Gold    :", GOLD_TABLE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Bronze (raw ingest from landing)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lower,
    max,
    sum as spark_sum,
    try_to_timestamp,
    when,
    window,
)

event_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

bronze_before = 0
try:
    bronze_before = spark.read.format("delta").load(BRONZE_TABLE_PATH).count()
except Exception:
    pass

bronze_stream = spark.readStream.schema(event_schema).json(LANDING_PATH)

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .start(BRONZE_TABLE_PATH)
)
bronze_query.awaitTermination()

bronze_after = spark.read.format("delta").load(BRONZE_TABLE_PATH).count()
bronze_added = bronze_after - bronze_before

print(f"Bronze complete — total rows: {bronze_after:,} (+{bronze_added:,} this run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Silver (clean + validate)

# COMMAND ----------

silver_before = 0
try:
    silver_before = spark.read.format("delta").load(SILVER_TABLE_PATH).count()
except Exception:
    pass

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

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start(SILVER_TABLE_PATH)
)
silver_query.awaitTermination()

silver_after = spark.read.format("delta").load(SILVER_TABLE_PATH).count()
silver_added = silver_after - silver_before
rejected_total = bronze_after - silver_after

print(f"Silver complete — total rows: {silver_after:,} (+{silver_added:,} this run)")
print(f"Rejected (Bronze - Silver): {rejected_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Gold (windowed metrics)

# COMMAND ----------

gold_before = 0
try:
    gold_before = spark.read.format("delta").load(GOLD_TABLE_PATH).count()
except Exception:
    pass

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
        "machine_id",
        "window_start",
        "window_end",
        "avg_temperature",
        "max_temperature",
        "avg_vibration",
        "event_count",
        "error_count",
        "is_overheating",
    )
)

gold_query = (
    gold_metrics.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .trigger(availableNow=True)
    .start(GOLD_TABLE_PATH)
)
gold_query.awaitTermination()

gold_after = spark.read.format("delta").load(GOLD_TABLE_PATH).count()
gold_added = gold_after - gold_before

print(f"Gold complete — total window rows: {gold_after:,} (+{gold_added:,} this run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Pipeline Health Summary
# MAGIC
# MAGIC This table powers **Dashboard Tile 1** (Pipeline Flow KPIs) and **Tile 2** (Data Quality Funnel).

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
silver_df = spark.read.format("delta").load(SILVER_TABLE_PATH)
gold_df = spark.read.format("delta").load(GOLD_TABLE_PATH)

quality_rate = round(100.0 * silver_after / bronze_after, 1) if bronze_after else 0.0

pipeline_health = spark.createDataFrame([(
    bronze_after,
    silver_after,
    gold_after,
    rejected_total,
    quality_rate,
    bronze_df.agg({"timestamp": "max"}).collect()[0][0],
    silver_df.agg({"event_time": "max"}).collect()[0][0],
    gold_df.agg({"window_end": "max"}).collect()[0][0],
    bronze_added,
    silver_added,
    gold_added,
)], [
    "bronze_rows",
    "silver_rows",
    "gold_window_rows",
    "rejected_rows",
    "quality_rate_pct",
    "last_bronze_timestamp",
    "last_silver_event_time",
    "last_gold_window_end",
    "bronze_added_this_run",
    "silver_added_this_run",
    "gold_added_this_run",
])

display(pipeline_health)

print()
print("=" * 60)
print("PIPELINE RUN COMPLETE")
print("=" * 60)
print(f"  Landing → Bronze : {bronze_after:>8,} rows  (+{bronze_added:,})")
print(f"  Bronze  → Silver : {silver_after:>8,} rows  (+{silver_added:,})")
print(f"  Silver  → Gold   : {gold_after:>8,} windows (+{gold_added:,})")
print(f"  Rejected         : {rejected_total:>8,} rows  ({100 - quality_rate:.1f}% filtered)")
print(f"  Quality rate     : {quality_rate:>7.1f}%")
print("=" * 60)
print("Open the dashboard — it should refresh with new data.")
print("Setup guide: docs/DASHBOARD_SETUP.md")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Quick sanity checks

# COMMAND ----------

print("Sample Bronze (includes corrupt rows):")
bronze_df.orderBy(col("timestamp").desc()).show(5, truncate=False)

print("Sample Silver (validated only):")
silver_df.orderBy(col("event_time").desc()).show(5, truncate=False)

print("Overheating alerts:")
gold_df.filter(col("is_overheating")).orderBy(col("window_start").desc()).show(5, truncate=False)

print("Machines with errors (latest windows):")
gold_df.filter(col("error_count") > 0).orderBy(col("window_start").desc()).show(5, truncate=False)
