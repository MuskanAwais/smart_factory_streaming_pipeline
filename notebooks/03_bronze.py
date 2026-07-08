# Databricks notebook source
# Module 4 — Bronze Streaming Layer
# Ingest raw JSON events into bronze_events (no cleaning, no filtering)

# COMMAND ----------

# Paths — update only if your volume layout differs
LANDING_PATH = "/Volumes/workspace/default/smart_factory/landing"
BRONZE_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/bronze_events"
CHECKPOINT_PATH = "/Volumes/workspace/default/smart_factory/checkpoints/bronze"

print("Landing:", LANDING_PATH)
print("Bronze table:", BRONZE_TABLE_PATH)
print("Checkpoint:", CHECKPOINT_PATH)

# COMMAND ----------

# AC-4.3 / AC-4.7 — Same explicit schema as Module 3 (no inference, no transforms)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# COMMAND ----------

# AC-4.1 — Read landing JSON as a stream
bronze_stream = (
    spark.readStream
    .schema(event_schema)
    .json(LANDING_PATH)
)

bronze_stream.printSchema()

# COMMAND ----------

# AC-4.2 / AC-4.4 — Write to Delta in append mode with checkpoint
# Free Edition uses serverless compute — ProcessingTime trigger is NOT supported.
# Use availableNow=True: process all new files since last checkpoint, then stop.
# Re-run this cell after uploading more JSON to simulate incremental ingestion.
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(BRONZE_TABLE_PATH)
)

bronze_query.awaitTermination()

print("Bronze batch processed.")
print("Last status:", bronze_query.lastProgress)

# COMMAND ----------

# AC-4.5 — Verify rows are landing in Bronze (run after stream has processed a batch)
bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

print("Bronze row count:", bronze_df.count())
bronze_df.show(10, truncate=False)

# COMMAND ----------

# AC-4.7 — Confirm corrupt rows are kept (null machine_id, bad status, etc.)
bronze_df.filter(
    (bronze_df.machine_id.isNull())
    | (bronze_df.status == "melting")
    | (bronze_df.timestamp == "abc")
    | (bronze_df.temperature > 150)
).show(truncate=False)

# COMMAND ----------

# AC-4.6 — Restart test (manual)
# 1. Upload a few NEW JSON files to landing (or run local producer + upload)
# 2. Re-run the "start stream" cell above — only new files are ingested (checkpoint)
# 3. Re-run verification cells — row count should increase without duplicating old rows
