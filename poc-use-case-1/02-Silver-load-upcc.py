# Databricks notebook source
dbutils.widgets.remove("database")

# COMMAND ----------

#dbutils.widgets.text("Schema", "tsel_dev")

# COMMAND ----------

Schema_Name=dbutils.widgets.get("Schema")
chkpt_path = f"/FileStore/telkomsel/checkpt/SILVER_UPCC_EDR"
src_table_name= f"{Schema_Name}.bronze_upcc_edr"
tgt_table_name= f"{Schema_Name}.SILVER_UPCC_EDR"

# COMMAND ----------

# DBTITLE 1,Read Delta Table
#Read bronze delta table as source
upcc_read=(spark.readStream
  .format("delta")
  .table(src_table_name))
  
#Convert dataframe to Temp view           
upcc_read.createOrReplaceTempView("v_upcc_read")
  

# COMMAND ----------

# DBTITLE 1,Trasformation Logic

upcc_transform=spark.sql("""select
  TRX_DATE,
  date_format(
    from_utc_timestamp(
      case
        when length(QUOTA_EXTENDED_ATTRIBUTE) > 12 then from_unixtime(
          conv(substr(QUOTA_EXTENDED_ATTRIBUTE, 3, 11), 16, 10) / 1000
        )
        else now()
      end,
      'Asia/Jakarta'
    ),
    'yyyy-MM-dd'
  ) PURCHASE_DATE,
  substring(QUOTA_EXTENDED_ATTRIBUTE, 19, 13) SKU,
  BRAND BRAND,
  L2_SERVICE_TYPE ALLOWANCE_TYPE,
  L3_ALLOWANCE_TYPE ALLOWANCE_SUB_TYPE,
  'UPCC' SOURCE,
  VALIDITY VALIDITY,
  case
    when (
      TRIGGER_TYPE = 18
      or (
        TRIGGER_TYPE = 2
        AND GROUP_SUB_ID IS NULL
        AND QUOTA_USAGE IS NOT NULL
      )
      or (
        TRIGGER_TYPE = 3
        AND GROUP_SUB_ID IS NULL
      )
    )
    AND ifrs_allowance_type in ('S', 'U')
    AND (
      DISC_VALIDITY IS NULL
      OR DISC_VALIDITY = 0
    )
    AND PRORATION_IND = 'N'
    AND ifrs15_ind_for_allowance_revenue = 1 then (QUOTA_USAGE * UNIT_PRICE)
    else 0
  end REV_PER_USAGE,
  case
    when TRIGGER_TYPE in (100, 116)
    AND ifrs_allowance_type in ('S', 'U')
    and QUOTA_FLAG = 0
    AND PRORATION_IND = 'N'
    AND ifrs15_ind_for_allowance_revenue = 1 then OLD_QUOTA_BALANCE * UNIT_PRICE
    when TRIGGER_TYPE in (136, 138, 111, 119)
    AND ifrs_allowance_type in ('S', 'U')
    AND PRORATION_IND = 'N'
    AND ifrs15_ind_for_allowance_revenue = 1 THEN QUOTA_BALANCE * UNIT_PRICE
    else 0
  end REV_SEIZED,
  split_part(IFRS_PARAM, "#", 4) ALLOCATED_TP,
  TRANSACTION_ID,
  case
    when QUOTA_USAGE IS NULL THEN 0
    when TRIGGER_TYPE IN (2, 3) THEN (
      case
        when GROUP_SUB_ID IS NULL THEN QUOTA_USAGE
        ELSE 0
      end
    )
    ELSE QUOTA_USAGE
  end QUOTA_USAGE,
  nvl(QUOTA_VALUE, 0) OFFER_QUOTA,
  now() LOAD_TS,
  'Databricks' LOAD_USER,
  1 JOB_ID,
  PRE_POST_FLAG,
  TRIGGER_TYPE,
  QUOTA_EXTENDED_ATTRIBUTE,
  GROUP_SUB_ID,
  ifrs_allowance_type,
  PRORATION_IND,
  DISC_VALIDITY,
  QUOTA_FLAG,
  OLD_QUOTA_BALANCE,
  IFRS_PARAM,
  event_date
from
  v_upcc_read""")

# COMMAND ----------


(upcc_transform.writeStream 
  .format("delta") 
  .option("checkpointLocation", chkpt_path)
  .option("mergeSchema", "true")
  .partitionBy("event_date")
  .trigger(availableNow=True)
  .table(tgt_table_name))
