# Databricks notebook source
# Module 3 — Databricks Setup and Spark Basics (batch only, no streaming)

# COMMAND ----------

# AC-3.2 — Confirm Spark works
spark.range(5).show()

# COMMAND ----------

LANDING_PATH = "/Volumes/workspace/default/smart_factory/landing"
display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

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

df = spark.read.schema(event_schema).json(LANDING_PATH)

df.printSchema()
df.show(10, truncate=False)
print("Total rows:", df.count())

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, when

# select
df_selected = df.select("machine_id", "temperature", "status", "timestamp")
df_selected.show(5, truncate=False)

# filter
df_hot = df.filter(col("temperature") > 70)
df_hot.show(5, truncate=False)

# withColumn
df_flagged = df.withColumn(
    "is_hot",
    when(col("temperature") > 75, True).otherwise(False)
)
df_flagged.select("machine_id", "temperature", "is_hot").show(5, truncate=False)

# groupBy + agg
df_agg = (
    df.groupBy("machine_id")
      .agg(
          avg("temperature").alias("avg_temperature"),
          count("*").alias("event_count")
      )
      .orderBy("machine_id")
)
df_agg.show(truncate=False)

# COMMAND ----------

df.groupBy("machine_id").count().orderBy("machine_id").show()