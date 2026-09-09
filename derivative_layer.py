# Databricks notebook source
df = spark.sql("select * from dbxtutorial.gold.gold_table where dbxtutorial.gold.gold_table.date > '2026-09-04'" )
# df.display()

# COMMAND ----------

# count_appliances = df.groupBy("appliances").count()
# display(count_appliances)

# count_appliances.write 
#     .format("delta") 
#     .mode("overwrite") 
#     .partitionBy("appliances") 
#     .saveAsTable("dbxtutorial.gold.count_appliances")

# count_sentiment = df.groupBy("sentiment").count()
# display(count_sentiment)

# count_sentiment.write 
#     .format("delta") 
#     .mode("overwrite") 
#     .partitionBy("sentiment") 
#     .saveAsTable("dbxtutorial.gold.count_sentiment")

# count_calltype = df.groupBy("call_type").count()
# display(count_calltype)

# count_calltype.write \
#     .format("delta") 
#     .mode("overwrite") 
#     .partitionBy("call_type")
#     .saveAsTable("dbxtutorial.gold.count_calltype")

# COMMAND ----------

count_appliances = df.groupBy("appliances").count()
# display(count_appliances)

(count_appliances.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("appliances")
    .saveAsTable("dbxtutorial.derivative.count_appliances"))

count_sentiment = df.groupBy("sentiment").count()
# display(count_sentiment)

(count_sentiment.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("sentiment")
    .saveAsTable("dbxtutorial.derivative.count_sentiment"))

count_calltype = df.groupBy("call_type").count()
# display(count_calltype)

(count_calltype.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("call_type")
    .saveAsTable("dbxtutorial.derivative.count_calltype"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE dbxtutorial.derivative.count_appliances;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE dbxtutorial.derivative.count_sentiment;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE dbxtutorial.derivative.count_calltype;