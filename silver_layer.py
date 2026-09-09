# Databricks notebook source
df= spark.sql("select * from dbxtutorial.bronze.bronze_table limit 200")
# display(df)


# COMMAND ----------

# %sql
# SELECT
#   *,
#   extracted_info.caller_name AS caller_name,
#   extracted_info.product_name AS product_name,
#   extracted_info.order_number AS order_number
# FROM (
#   SELECT
#     *,
#     ai_extract(transcript, array('caller_name', 'product_name', 'order_number')) AS extracted_info
#   FROM dbxtutorial.bronze.bronze_table

# )

# COMMAND ----------

# %sql
# SELECT
#   *,
#   ai_classify(transcript, array('complaint', 'order placement', 'customer query')) AS call_type
# FROM (
#   SELECT
#     *,
#     extracted_info.caller_name AS caller_name,
#     extracted_info.product_name AS product_name,
#     extracted_info.order_number AS order_number
#   FROM (
#     SELECT
#       *,
#       ai_extract(transcript, array('caller_name', 'product_name', 'order_number')) AS extracted_info
#     FROM dbxtutorial.bronze.bronze_table
#   )
# )

# COMMAND ----------

from pyspark.sql import functions as F

df_final = df.withColumn(
    "extracted_info",
    F.expr("ai_extract(transcript, array('caller_name', 'product_name', 'order_number'))")
).withColumn(
    "call_type",
    F.expr("ai_classify(transcript, array('complaint', 'order placement', 'customer query'))")
).withColumn(
    "sentiment",
    F.expr("ai_classify(transcript, array('positive', 'neutral', 'negative', 'angry', 'happy'))")
)

df_final = df_final.select(
    "*",
    F.col("extracted_info")["caller_name"].alias("caller_name"),
    F.col("extracted_info")["product_name"].alias("product_name"),
    F.col("extracted_info")["order_number"].alias("order_number")
).drop("extracted_info")

# display(df_final)

# COMMAND ----------

from pyspark.sql import functions as F

df_final = df_final.withColumnRenamed("uuid", "uuid_bronze") \
                    .withColumnRenamed("insertion_time", "timestamp_bronze")

df_final = df_final.withColumn(
    "uuid_silver",
    F.sha2(F.concat_ws("||", F.col("caller_name"), F.col("product_name"), F.col("order_number")), 256)
).withColumn(
    "timestamp_silver",
    F.current_timestamp()
)

# display(df_final)

# COMMAND ----------

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbxtutorial.silver.silver_table")