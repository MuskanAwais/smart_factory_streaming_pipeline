# Databricks notebook source
# Module 0 — One-Time Workspace Setup
#
# Run this ONCE after importing notebooks to Databricks.
# Creates volume folders, pipeline_runs table, and SQL views for the dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC # Smart Factory — One-Time Setup
# MAGIC
# MAGIC Run this notebook **once** before the automated pipeline.
# MAGIC
# MAGIC **Prerequisite:** Create the Unity Catalog volume `smart_factory` in Catalog Explorer if it does not exist:
# MAGIC - Catalog: `workspace`
# MAGIC - Schema: `default`
# MAGIC - Volume name: `smart_factory`

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

folders = [
    LANDING_PATH,
    f"{VOLUME_BASE}/tables",
    f"{VOLUME_BASE}/checkpoints",
    CHECKPOINT_BRONZE,
    CHECKPOINT_SILVER,
    CHECKPOINT_GOLD,
]

for folder in folders:
    dbutils.fs.mkdirs(folder)
    print(f"OK  {folder}")

print("\nAll folders created.")

# COMMAND ----------

# Initialize empty Delta tables so SQL views work before first pipeline run
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, DoubleType, BooleanType,
)

bronze_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

silver_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

gold_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("avg_temperature", DoubleType(), True),
    StructField("max_temperature", DoubleType(), True),
    StructField("avg_vibration", DoubleType(), True),
    StructField("event_count", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("is_overheating", BooleanType(), True),
])

runs_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("landing_files_added", LongType(), False),
    StructField("bronze_rows_added", LongType(), False),
    StructField("silver_rows_added", LongType(), False),
    StructField("gold_windows_added", LongType(), False),
    StructField("bronze_rows_total", LongType(), False),
    StructField("silver_rows_total", LongType(), False),
    StructField("gold_windows_total", LongType(), False),
    StructField("rejected_rows_total", LongType(), False),
    StructField("quality_rate_pct", DoubleType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("status", StringType(), False),
])

def _ensure_delta_table(path, schema, label):
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"OK  {label} exists ({count} rows)")
    except Exception:
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
        print(f"OK  {label} created (empty)")

_ensure_delta_table(BRONZE_TABLE_PATH, bronze_schema, "bronze_events")
_ensure_delta_table(SILVER_TABLE_PATH, silver_schema, "silver_events")
_ensure_delta_table(GOLD_TABLE_PATH, gold_schema, "gold_machine_metrics")
_ensure_delta_table(PIPELINE_RUNS_PATH, runs_schema, "pipeline_runs")

# COMMAND ----------

# SQL views for dashboard (re-run safe)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.bronze_events AS
SELECT * FROM delta.`{BRONZE_TABLE_PATH}`
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.silver_events AS
SELECT * FROM delta.`{SILVER_TABLE_PATH}`
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_machine_metrics AS
SELECT * FROM delta.`{GOLD_TABLE_PATH}`
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.pipeline_runs AS
SELECT * FROM delta.`{PIPELINE_RUNS_PATH}`
""")

print("SQL views created:")
print(f"  {CATALOG}.{SCHEMA}.bronze_events")
print(f"  {CATALOG}.{SCHEMA}.silver_events")
print(f"  {CATALOG}.{SCHEMA}.gold_machine_metrics")
print(f"  {CATALOG}.{SCHEMA}.pipeline_runs")

# COMMAND ----------

# pipeline_health view — row counts across all medallion layers
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.pipeline_health AS
SELECT
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_events) AS bronze_rows,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_events) AS silver_rows,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_machine_metrics) AS gold_window_rows,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_events)
      - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_events) AS rejected_rows,
    ROUND(100.0 * (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_events)
      / NULLIF((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bronze_events), 0), 1) AS quality_rate_pct,
    (SELECT MAX(timestamp) FROM {CATALOG}.{SCHEMA}.bronze_events) AS last_bronze_timestamp,
    (SELECT MAX(event_time) FROM {CATALOG}.{SCHEMA}.silver_events) AS last_silver_event_time,
    (SELECT MAX(window_end) FROM {CATALOG}.{SCHEMA}.gold_machine_metrics) AS last_gold_window_end,
    (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.pipeline_runs) AS last_pipeline_run
""")

print(f"  {CATALOG}.{SCHEMA}.pipeline_health")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. Run `07_auto_pipeline.py` once manually to verify
# MAGIC 2. Schedule `07_auto_pipeline.py` as a Job every 2 minutes — see `docs/JOB_SETUP.md`
# MAGIC 3. Build the dashboard — see `docs/DASHBOARD_SETUP.md`

# COMMAND ----------

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print("Next: run 07_auto_pipeline.py")
print("Then: schedule as Job (docs/JOB_SETUP.md)")
print("Then: build dashboard (docs/DASHBOARD_SETUP.md)")
