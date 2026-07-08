# Databricks notebook source
# Module 5 — Silver Cleaning Layer
# Read bronze_events, clean/validate, write silver_events (drop bad rows)

# COMMAND ----------

BRONZE_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/bronze_events"
SILVER_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/silver_events"
CHECKPOINT_PATH = "/Volumes/workspace/default/smart_factory/checkpoints/silver"

VALID_STATUSES = ["running", "idle", "error"]

print("Bronze source:", BRONZE_TABLE_PATH)
print("Silver table:", SILVER_TABLE_PATH)
print("Checkpoint:", CHECKPOINT_PATH)

# COMMAND ----------

# AC-5.1 — Read Bronze as a stream
bronze_stream = spark.readStream.format("delta").load(BRONZE_TABLE_PATH)

bronze_stream.printSchema()

# COMMAND ----------

# AC-5.2 / AC-5.3 / AC-5.4 — Cast, parse timestamp, normalize status
# Use try_to_timestamp so corrupt values like 'abc' become NULL (then filtered out)
from pyspark.sql.functions import col, lower, try_to_timestamp

silver_stream = (
    bronze_stream
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("humidity", col("humidity").cast("double"))
    .withColumn("vibration", col("vibration").cast("double"))
    .withColumn("status", lower(col("status")))
    .withColumn("event_time", try_to_timestamp(col("timestamp")))
)

# COMMAND ----------

# AC-5.5 / AC-5.6 / AC-5.7 — Apply REQ-DATA-2 validation rules (drop invalid rows)
silver_clean = silver_stream.filter(
    col("machine_id").isNotNull()
    & col("temperature").between(-20, 150)
    & col("humidity").between(0, 100)
    & col("vibration").between(0, 50)
    & col("status").isin(VALID_STATUSES)
    & col("event_time").isNotNull()
).select(
    "machine_id",
    "temperature",
    "humidity",
    "vibration",
    "status",
    "event_time",
)

silver_clean.printSchema()

# COMMAND ----------

# Write to Silver Delta table (append + checkpoint)
# Free Edition: use availableNow=True (ProcessingTime not supported on serverless)
silver_query = (
    silver_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(SILVER_TABLE_PATH)
)

silver_query.awaitTermination()

print("Silver batch processed.")
print("Last status:", silver_query.lastProgress)

# COMMAND ----------

# Verification — Silver row counts and sample
bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)
silver_df = spark.read.format("delta").load(SILVER_TABLE_PATH)

bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Rows removed:", bronze_count - silver_count)

silver_df.show(10, truncate=False)

# COMMAND ----------

# AC-5.7 — Corrupt rows should NOT be in Silver
silver_df.filter(
    col("machine_id").isNull()
    | col("status").isin(["melting"])
    | col("temperature") > 150
    | col("event_time").isNull()
).count()

# Expect 0 corrupt rows in Silver

# COMMAND ----------

# AC-5.6 — No nulls in required Silver columns
from pyspark.sql.functions import sum as spark_sum

null_check = silver_df.select(
    [
        spark_sum(col(c).isNull().cast("int")).alias(f"{c}_nulls")
        for c in ["machine_id", "temperature", "humidity", "vibration", "status", "event_time"]
    ]
)
display(null_check)

# Expect all null counts = 0
