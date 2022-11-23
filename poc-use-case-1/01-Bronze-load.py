# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook is to load the data from S3 bucket to Databricks Lakehouse Bronze layer 

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %md
# MAGIC ##Define parameter

# COMMAND ----------

file_name=dbutils.widgets.get("file")
Schema_Name=dbutils.widgets.get("Schema")
Table_Name=dbutils.widgets.get("Table")
Full_table_path=Full_table_path=f"{Schema_Name}.{Table_Name}" #f"hive_metastore.{Schema_Name}.{Table_Name}"
input_data_path = f"s3a://landing-poc/{file_name}/event_date=2022-07-31/"
chkpt_path = f"/FileStore/telkomsel/checkpt/{Schema_Name}/{file_name}"

print(input_data_path)
print(chkpt_path)
print(Full_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Data to Bronze tables using Autoloader

# COMMAND ----------

# DBTITLE 1,Load Data to Bronze tables using Autoloader
from  pyspark.sql.functions import input_file_name
df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", chkpt_path)
      .option("cloudFiles.partitionColumns", "event_date")
      .load(input_data_path)
      .withColumn("filePath",input_file_name())
      .withColumn("Load_timestamp",input_file_name()))

(df.writeStream.format("delta")
 .trigger(availableNow=True)
 .option("checkpointLocation", chkpt_path)
 .partitionBy("event_date")
 .table(Full_table_path))
