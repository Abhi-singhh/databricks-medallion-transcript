# Databricks notebook source
# CSV
df = spark.read.csv("/Volumes/dbxtutorial/bronze/bronze_volume/call_transcripts.csv", header=True, inferSchema=True)

# df.display()

# COMMAND ----------

from pyspark.sql import functions as F

df_with_uuid = df.withColumn(
    "uuid",
    F.sha2(F.concat_ws("||", F.col("file_name"), F.col("transcript")), 256)
)

# df_with_uuid.display()

# COMMAND ----------

from pyspark.sql import functions as F

df_with_uuid = df_with_uuid.withColumn(
    "insertion_time",
    F.current_timestamp()
)

df_with_uuid.display()

# COMMAND ----------

df_with_uuid.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbxtutorial.bronze.bronze_table")