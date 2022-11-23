# Databricks notebook source
#dbutils.widgets.text("Schema", "tsel_dev")

# COMMAND ----------

Schema_Name=dbutils.widgets.get("Schema")
chkpt_path = f"/FileStore/telkomsel/checkpt/{Schema_Name}/SILVER_TC_CHG"
src_table_name= f"{Schema_Name}.bronze_tc_chg"
tgt_table_name= f"{Schema_Name}.SILVER_TC_CHG"

# COMMAND ----------

#Read bronze delta table as source
tc_chg_read=(spark.readStream
  .format("delta")
  .table(src_table_name))
  
#Convert dataframe to Temp view           
tc_chg_read.createOrReplaceTempView("v_tc_chg_read")
  

# COMMAND ----------

tc_chg_transform=spark.sql("""
select
  TRX_DATE,
  date_format(to_date(timestamp_ifrs, 'yyyyMMddHHmmss'),'yyyy-MM-dd') PURCHASE_DATE,
  SUBSTRING(TRANSACTION_ID, 19, 13) SKU,
  BRAND,
  L2_SERVICE_TYPE ALLOWANCE_TYPE,
  L3_ALLOWANCE_TYPE ALLOWANCE_SUB_TYPE,
  'CHG' SOURCE,
  VALIDITY VALIDITY,
  case
    when ifrs_ind_for_allowance_revenue in (1, 2, 3)
    AND ALLOWANCE_CONSUMED_REVENUE IS NOT NULL
    AND (
      HISTORICAL_QUOTA = 0
      OR HISTORICAL_QUOTA IS NULL
    ) then (
      case
        when RATE_PER_UNIT = -1 THEN EARNED_REVENUE
        ELSE (UNIT_PRICE * EVENT_ALLOWANCE_CONSUMED)
      end
    )
    else 0
  end REV_PER_USAGE,
  0 REV_SEIZED,
  split_part(ALLOWANCE_CONSUMED_REVENUE,"#",16)  ALLOCATED_TP, --derive
  split_part(ALLOWANCE_CONSUMED_REVENUE,"#",9) QUOTA_USAGE, --derive
  cast(split_part(ALLOWANCE_CONSUMED_REVENUE,"#",8) as bigint) OFFER_QUOTA,--derive
    TRANSACTION_ID,
    to_date(load_ts) LOAD_DATE,
 now() LOAD_TS,
'Databricks' LOAD_USER,
1 JOB_ID,
  timestamp_ifrs,
  HISTORICAL_QUOTA,
  ifrs_ind_for_allowance_revenue,
 ALLOWANCE_CONSUMED_REVENUE ,
  RATE_PER_UNIT,
  EARNED_REVENUE,
  UNIT_PRICE,
  EVENT_ALLOWANCE_CONSUMED,
  event_date
from v_tc_chg_read""")
  
  
  

# COMMAND ----------


(tc_chg_transform.writeStream 
  .format("delta") 
  .option("checkpointLocation", chkpt_path)
  .option("mergeSchema", "true")
  .partitionBy("event_date")
  .trigger(availableNow=True)
  .table(tgt_table_name))
