# Databricks notebook source
df=spark.sql('select * from dbxtutorial.silver.silver_table')
# df.display()


# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn(
    "appliances",
    F.expr("ai_classify(product_name, array('kitchen', 'home', 'tech electronic', 'software', 'bathroom', 'sports'))")
)

# display(df)

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn(
    "uuid_gold",
    F.sha2(
        F.concat_ws("||",
            F.col("file_name"),
            F.col("transcript"),
            F.col("call_type"),
            F.col("sentiment"),
            F.col("caller_name"),
            F.col("product_name"),
            F.col("order_number"),
            F.col("appliances")
        ),
        256
    )
).withColumn(
    "timestamp_gold",
    F.current_timestamp()
)

# display(df)

# COMMAND ----------


df = df.withColumn(
    "date",
    F.to_date(F.col("timestamp_gold"))
)

# display(df)

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("dbxtutorial.gold.gold_table")