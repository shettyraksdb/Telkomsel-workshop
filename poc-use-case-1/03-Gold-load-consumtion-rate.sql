-- Databricks notebook source
--CREATE WIDGET TEXT report_date DEFAULT '2022-08-01';
--CREATE WIDGET TEXT schema DEFAULT 'tsel_dev';

-- COMMAND ----------

use catalog hive_metastore

-- COMMAND ----------

create
or replace temp view v_silver_wisdom as
select
  *
from
  ${schema}.silver_wisdom WISDOM
where
  status = 'OK00'
  and LENGTH(WISDOM.TRANSACTION_ID) >= 32
  and WISDOM.EVENT_DATE between to_date('${report_date}') -7
  and to_date('${report_date}')

-- COMMAND ----------

create
or replace temp view v_CONSUMPTION_RATE_tc as
select
  tc.TRX_DATE,
  tc.PURCHASE_DATE,
  tc.SKU,
  tc.BRAND,
  tc.ALLOWANCE_TYPE,
  tc.ALLOWANCE_SUB_TYPE,
  tc.SOURCE,
  tc.VALIDITY,
  tc.PURCHASE_DATE + tc.VALIDITY EXPIRY_DATE,
  sum(tc.REV_PER_USAGE) REV_PER_USAGE,
  sum(tc.REV_SEIZED) REV_SEIZED,
  SUM(tc.REV_PER_USAGE) REV_ALL_IFRS,
  transaction_price PRICE,
  ALLOCATED_TP,
  sum(ALLOCATED_TP) TOTAL_ALLOCATED_TP_PER_SKU,
  count(distinct wisdom.TRANSACTION_ID) TRX,
  sum (tc.QUOTA_USAGE) QUOTA_USAGE,
  OFFER_QUOTA,
  sum(QUOTA_USAGE) /(
    OFFER_QUOTA * count(distinct wisdom.TRANSACTION_ID)
  ) as CONSUMPTION_RATE,
  now() LOAD_TS,
  'Databricks' LOAD_USER,
  tc.EVENT_DATE EVENT_DATE,
  '1' JOB_ID
from
  ${schema}.silver_tc_chg tc
  join v_silver_wisdom WISDOM 
  on  tc.PURCHASE_DATE = WISDOM.EVENT_DATE
 AND tc.TRANSACTION_ID = WISDOM.TRANSACTION_ID
where
  tc.LOAD_DATE between to_date('${report_date}') -7
  and to_date('${report_date}')
group by
  tc.TRX_DATE,
  tc.PURCHASE_DATE,
  tc.SKU,
  tc.BRAND,
  tc.ALLOWANCE_TYPE,
  tc.ALLOWANCE_SUB_TYPE,
  tc.SOURCE,
  tc.VALIDITY,
  tc.ALLOCATED_TP,
  tc.OFFER_QUOTA,
  wisdom.transaction_price,
  tc.EVENT_DATE 

-- COMMAND ----------

create table ${schema}.gold_CONSUMPTION_RATE_DD PARTITIONED BY (EVENT_DATE)
select * from v_CONSUMPTION_RATE_tc


