# Databricks notebook source
# DBTITLE 1,Import Libraries
#from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create & Save table
print("#######>>> Creating a table <<<#######")
data = spark.range(0,5)
data.write.format("delta").save("/tmp/delta-table")

# COMMAND ----------

# DBTITLE 1,Check data storage
# MAGIC %fs ls tmp/delta-table

# COMMAND ----------

# DBTITLE 1,Folder containing metadata
# MAGIC %fs ls tmp/delta-table/_delta_log 

# COMMAND ----------

# DBTITLE 1,View metadata in JSON file
#To download file go here: https://community.cloud.databricks.com/dbfs/tmp/delta-table/_delta_log/00000000000000000000.json?o=2296553551556281#

jsonMeta = spark.read.format("json").option("multiline","true").load("/tmp/delta-table/_delta_log/00000000000000000000.json")
display(jsonMeta)

# COMMAND ----------

# MAGIC %scala
# MAGIC val mdf = spark.read.option("multiline", "false").json("/tmp/delta-table/_delta_log/00000000000000000000.json")
# MAGIC display(mdf)

# COMMAND ----------

# DBTITLE 1,Read table
print("#######>>> Reading the table <<<#######")
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

# COMMAND ----------

display(df)  #id is default name given, all dataframes have named columns in Spark

# COMMAND ----------

# DBTITLE 1,Update table with new data
print("#######>>> Updating the table <<<#######")
data2 = spark.range(5,10)
appndedDf = df.union(data2)
appndedDf.show()

# COMMAND ----------

# DBTITLE 1,Append table in DBFS
#historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events/")
data2.write.format("delta").mode("append").save("/tmp/delta-table")

# COMMAND ----------

# DBTITLE 1,Check file storage
# MAGIC %fs ls /tmp/delta-table

# COMMAND ----------

# MAGIC %fs ls /tmp/delta-table/_delta_log

# COMMAND ----------

#Download new JSON file: https://community.cloud.databricks.com/dbfs/tmp/delta-table/_delta_log/00000000000000000001.json?o=2296553551556281#

# COMMAND ----------

# DBTITLE 1,Count rows
df.count()#Row count increased from 5 to 10 to 15 (over 3 runs)

# COMMAND ----------

# DBTITLE 1,View new updated table
#df.show()
display(df)

# COMMAND ----------

# DBTITLE 1,Optimise the Delta Table
#Optimise significantly improves read and execution times of Delta Tables (5-10X)
display(spark.sql("DROP TABLE  IF EXISTS df"))

display(spark.sql("CREATE TABLE df USING DELTA LOCATION '/tmp/delta-table/'"))

display(spark.sql("OPTIMIZE df"))


# COMMAND ----------

# DBTITLE 1,Check execution time of optimised delta table
display(df) #~1.6s vs 5s on non-optimised delta table

# COMMAND ----------

# DBTITLE 1,View Table History
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY '/tmp/delta-table/'

# COMMAND ----------

# DBTITLE 1,Time travel back in history
df_old = spark.read.format("delta").option("versionAsOf",0).load("/tmp/delta-table")
#df_old.show()
display(df_old)