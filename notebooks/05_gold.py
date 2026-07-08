# Databricks notebook source
# Module 6 — Gold Analytics Layer
# Windowed per-machine metrics from silver_events

# COMMAND ----------

SILVER_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/silver_events"
GOLD_TABLE_PATH = "/Volumes/workspace/default/smart_factory/tables/gold_machine_metrics"
CHECKPOINT_PATH = "/Volumes/workspace/default/smart_factory/checkpoints/gold"

print("Silver source:", SILVER_TABLE_PATH)
print("Gold table:", GOLD_TABLE_PATH)
print("Checkpoint:", CHECKPOINT_PATH)

# COMMAND ----------

# AC-6.1 — Read Silver as a stream
silver_stream = spark.readStream.format("delta").load(SILVER_TABLE_PATH)

silver_stream.printSchema()

# COMMAND ----------

# AC-6.2 / AC-6.3 / AC-6.4 / AC-6.5 / AC-6.6 — Watermark, window, aggregations, overheating flag
from pyspark.sql.functions import avg, col, count, max, sum as spark_sum, when, window

gold_metrics = (
    silver_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        col("machine_id"),
        window(col("event_time"), "1 minute"),
    )
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

gold_metrics.printSchema()

# COMMAND ----------

# Write to Gold Delta table
# SPEC target: output mode update — Delta on Free Edition serverless does not support update.
# Use append with watermark + window: finalized window rows are appended (correct for this pattern).
gold_query = (
    gold_metrics.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(GOLD_TABLE_PATH)
)

gold_query.awaitTermination()

print("Gold batch processed.")
print("Last status:", gold_query.lastProgress)

# COMMAND ----------

# Verification — Gold metrics sample
gold_df = spark.read.format("delta").load(GOLD_TABLE_PATH)

print("Gold row count:", gold_df.count())
gold_df.orderBy("window_start", "machine_id").show(20, truncate=False)

# COMMAND ----------

# AC-6.5 — Overheating alerts (max_temperature > 85)
gold_df.filter(col("is_overheating") == True).show(truncate=False)

# COMMAND ----------

# AC-6.7 — Machines with errors in a window
gold_df.filter(col("error_count") > 0).orderBy("window_start", "machine_id").show(truncate=False)
