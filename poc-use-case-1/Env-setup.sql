-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("Name", "tsel_dev","Please enter your name:")

-- COMMAND ----------

use CATALOG hive_metastore

-- COMMAND ----------

create schema  ${Name}

-- COMMAND ----------

drop schema ${Name} CASCADE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC Name=dbutils.widgets.get("Name")
-- MAGIC path=f"dbfs:/FileStore/telkomsel/checkpt/{Name}_db"
-- MAGIC print(path)
-- MAGIC dbutils.fs.rm(path,True)
