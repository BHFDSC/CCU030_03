-- Databricks notebook source
-- MAGIC %md **HES: LTCs**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

-- MAGIC %md **(1) APC:**

-- COMMAND ----------

describe .ccu030_${dt}_hes_apc_all_years

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for dataset in ["apc", "op"]:
-- MAGIC   spark.sql(f"CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_{dt}_hes_{dataset}_ltc_temp  AS SELECT t.*, t2.covid_date, t3.death_date FROM .ccu030_{dt}_hes_{dataset}_all_years t LEFT JOIN .ccu030_{dt}_covid_severity t2 ON t.person_id_deid = t2.person_id_deid LEFT JOIN .ccu030_{dt}_tmp_deaths t3 ON t.person_id_deid = t3.person_id_deid") 

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_hes_apc_ltc_temp as
select *, 
  case
  when covid_date is not null then covid_date
  when covid_date is null and death_date < '2020-12-31' then death_date
  else '2020-12-31'
  end as mydate
  from global_temp.ccu030_${dt}_hes_apc_ltc_temp

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_hes_apc_ltc_temp as
select * 
from global_temp.ccu030_${dt}_hes_apc_ltc_temp
where ADMIDATE >= date_sub(mydate, 365) AND ADMIDATE <= mydate

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_hes_op_ltc_temp as
select *, 
  case
  when covid_date is not null then covid_date
  when covid_date is null and death_date < '2020-12-31' then death_date
  else '2020-12-31'
  end as mydate
  from global_temp.ccu030_${dt}_hes_op_ltc_temp

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_hes_op_ltc_temp as
select * 
from global_temp.ccu030_${dt}_hes_op_ltc_temp
where APPTDATE >= date_sub(mydate, 365) AND APPTDATE <= mydate

-- COMMAND ----------

describe global_temp.ccu030_${dt}_hes_op_ltc_temp

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_hes_apc_ltc_temp  AS 
SELECT person_id_deid, stack(20, DIAG_4_01, DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, DIAG_4_19, DIAG_4_20) AS diag_4 
FROM global_temp.ccu030_${dt}_hes_apc_ltc_temp

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_hes_op_ltc_temp  AS 
SELECT person_id_deid, stack(20, DIAG_4_01, DIAG_4_02, DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, DIAG_4_11, DIAG_4_12) AS diag_4 
FROM global_temp.ccu030_${dt}_hes_op_ltc_temp

-- COMMAND ----------

-- MAGIC %md link with snomed/icd10 table of LTCs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for dataset in ["apc", "op"]:
-- MAGIC   spark.sql(f"CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_{dt}_hes_{dataset}_ltc_temp  AS SELECT t.*, t2.cond_abbreviation FROM global_temp.ccu030_{dt}_hes_{dataset}_ltc_temp t LEFT JOIN .ccu030_{dt}_cam_cprdcam_mc_snomedct_final t2 ON t.diag_4 = t2.icd10")

-- COMMAND ----------

-- MAGIC %md loop over LTCs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import pyspark.sql.functions as f
-- MAGIC 
-- MAGIC items = [
-- MAGIC   'ALC',
-- MAGIC   'AB',
-- MAGIC   'ANX',
-- MAGIC   'AST',
-- MAGIC   'ATR',
-- MAGIC   'BLI',
-- MAGIC   'BRO',
-- MAGIC   'CAN',
-- MAGIC   'CHD',
-- MAGIC   'CKD',
-- MAGIC   'CLD',
-- MAGIC   'CSP',
-- MAGIC   'COPD',
-- MAGIC   'DEM', 
-- MAGIC   'DEP',
-- MAGIC   'DIA',
-- MAGIC   'DIV',
-- MAGIC   'EPI',
-- MAGIC   'HF',
-- MAGIC   'HL',
-- MAGIC   'HYP',
-- MAGIC   'IBD',
-- MAGIC   'IBS',
-- MAGIC   'MIG',
-- MAGIC   'MS',
-- MAGIC   'PUD',
-- MAGIC   'PNC',
-- MAGIC   'PRK',
-- MAGIC   'PSD',
-- MAGIC   'PSM',
-- MAGIC   'PSO',
-- MAGIC   'PVD',
-- MAGIC   'RHE',
-- MAGIC   'SCZ',
-- MAGIC   'SIN',
-- MAGIC   'STR',
-- MAGIC   'THY']
-- MAGIC     
-- MAGIC for dataset in ["apc", "op"]:  
-- MAGIC   results = (
-- MAGIC   spark.table(f"global_temp.ccu030_{dt}_hes_{dataset}_ltc_temp")
-- MAGIC   .groupby(f.col("person_id_deid").alias(f"person_id_deid_{dataset}"))
-- MAGIC   .agg(*(f.when(f.array_contains(f.collect_set(f.col("cond_abbreviation")), item), 1).otherwise(0).alias(f"{item}_{dataset}")
-- MAGIC          for item in items))
-- MAGIC   )
-- MAGIC   
-- MAGIC   #   so at this point the datasets have unique records
-- MAGIC   # keep only relevant columns
-- MAGIC 
-- MAGIC   mycols = [col for col in results.columns if f'_{dataset}' in col]
-- MAGIC 
-- MAGIC   results = results.select(mycols)
-- MAGIC   
-- MAGIC   results.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_hes_{dataset}_ltc_final")
