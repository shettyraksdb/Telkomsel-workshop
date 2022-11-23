# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

#dbutils.widgets.text("Schema", "tsel_dev")

# COMMAND ----------

Schema_Name=dbutils.widgets.get("Schema")
chkpt_path = f"/FileStore/telkomsel/checkpt/{Schema_Name}/SILVER_wisdom"
src_table_name= f"{Schema_Name}.bronze_wisdom"
tgt_table_name= f"{Schema_Name}.SILVER_WISDOM"

# COMMAND ----------

# DBTITLE 1,Read Delta Table
#Read bronze delta table as source
upcc_read=(spark.readStream
  .format("delta")
  .table(src_table_name))
  
#Convert dataframe to Temp view           
upcc_read.createOrReplaceTempView("v_wisdom_read")
  

# COMMAND ----------

# DBTITLE 1,Trasformation Logic

wisdom_transform=spark.sql("""select
  TRX_DATE,
  TRANSACTION_ID,
  STATUS,
  transaction_price,
  event_date
from
  v_wisdom_read""")

# COMMAND ----------

# DBTITLE 1,Write to Sink

(wisdom_transform.writeStream 
  .format("delta") 
  .option("checkpointLocation", chkpt_path)
  .option("mergeSchema", "true")
  .partitionBy("event_date")
  .trigger(availableNow=True)
  .table(tgt_table_name))
