-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Vaccination status

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_vaccine_status as
SELECT PERSON_ID_DEID, VACCINE_PRODUCT_CODE, cast(DOSE_SEQUENCE AS INT) AS N_DOSES, to_date(SUBSTRING(DATE_AND_TIME, 1, 8), 'yyyyMMdd') as DATE
FROM .ccu030_${dt}_vaccine_status

-- COMMAND ----------

describe  global_temp.ccu030_${dt}_vaccine_status

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_vaccine_status  AS
SELECT G.*, S.covid_date 
  FROM global_temp.ccu030_${dt}_vaccine_status G
  LEFT JOIN .ccu030_${dt}_covid_severity S ON G.person_id_deid = S.person_id_deid 

-- COMMAND ----------

-- drop vaccination events after the covid date
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_vaccine_status  AS
SELECT *
  FROM global_temp.ccu030_${dt}_vaccine_status 
  where DATE < covid_date   

-- COMMAND ----------

-- keep the latest date for each record
-- DROP TABLE IF EXISTS .ccu030_${dt}_vaccine_status2;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_vaccine_status2 USING PARQUET AS
WITH cte AS
(
   SELECT *,
         ROW_NUMBER() OVER (PARTITION BY person_id_DEID ORDER BY DATE DESC) AS rn
   FROM global_temp.ccu030_${dt}_vaccine_status 
)
SELECT person_id_deid, n_doses, VACCINE_PRODUCT_CODE
FROM cte
WHERE rn = 1;
ALTER TABLE .ccu030_${dt}_vaccine_status2 OWNER TO 

-- COMMAND ----------


