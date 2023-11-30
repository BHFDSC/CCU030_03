-- Databricks notebook source
-- MAGIC %python
-- MAGIC my_production_date = dbutils.widgets.get("my_production_date")
-- MAGIC my_pc_meds_production_date = dbutils.widgets.get("my_pc_meds_production_date")
-- MAGIC dt = dbutils.widgets.get("dt")
-- MAGIC print(dt)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for dataset in ["apc", "op", "cc", "ae"]:
-- MAGIC   spark.sql(f"""
-- MAGIC             create table if not exists .ccu030_{dt}_hes_{dataset}_all_years using parquet as 
-- MAGIC             select *  
-- MAGIC             from .hes_{dataset}_all_years_archive 
-- MAGIC             where ProductionDate == '{my_production_date}'
-- MAGIC             """)
-- MAGIC   spark.sql(f"""
-- MAGIC             ALTER TABLE .ccu030_{dt}_hes_{dataset}_all_years OWNER TO 
-- MAGIC             """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC           create table if not exists .ccu030_{dt}_primary_care_meds2 using parquet as 
-- MAGIC           select * 
-- MAGIC           from .primary_care_meds__archive 
-- MAGIC           where ProductionDate == '{my_pc_meds_production_date}'
-- MAGIC           ''')
-- MAGIC spark.sql(f"ALTER TABLE .ccu030_{dt}_primary_care_meds2 OWNER TO ")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for dataset in ["gdppr", "chess", "deaths", "sgss", "vaccine_status"]:
-- MAGIC     spark.sql(f"""
-- MAGIC             create table if not exists .ccu030_{dt}_{dataset} using parquet as 
-- MAGIC             select * 
-- MAGIC             from .{dataset}__archive 
-- MAGIC             where ProductionDate == '{my_production_date}'
-- MAGIC             """)
-- MAGIC     spark.sql(f"""
-- MAGIC             ALTER TABLE .ccu030_{dt}_{dataset} OWNER TO 
-- MAGIC             """)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS .ccu030_${dt}_ethnicity_lookup USING PARQUET AS
SELECT *, 
      CASE WHEN ETHNICITY_CODE IN ('1','2','3','N','M','P') THEN "Black or Black British"
           WHEN ETHNICITY_CODE IN ('0','A','B','C') THEN "White"
           WHEN ETHNICITY_CODE IN ('4','5','6','L','K','J','H') THEN "Asian or Asian British"
           WHEN ETHNICITY_CODE IN ('7','8','W','T','S','R') THEN "Other Ethnic Groups"
           WHEN ETHNICITY_CODE IN ('D','E','F','G') THEN "Mixed"
           WHEN ETHNICITY_CODE IN ('9','Z','X') THEN "Unknown"
           ELSE 'Unknown' END as ETHNIC_GROUP  
FROM (
  SELECT ETHNICITY_CODE, ETHNICITY_DESCRIPTION FROM dss_corporate.hesf_ethnicity
  UNION ALL
  SELECT Value as ETHNICITY_CODE, Label as ETHNICITY_DESCRIPTION FROM dss_corporate.gdppr_ethnicity WHERE Value not in (SELECT ETHNICITY_CODE FROM FROM dss_corporate.hesf_ethnicity));
ALTER TABLE .ccu030_${dt}_ethnicity_lookup OWNER TO 
-- this table has unique records

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS .ccu030_${dt}_snomed_icd10_lookup USING PARQUET AS
select distinct REFERENCED_COMPONENT_ID as snomed, MAP_TARGET as icd10 
from dss_corporate.snomed_ct_rf2_map_to_icd10_v01;
ALTER TABLE .ccu030_${dt}_snomed_icd10_lookup OWNER TO 

-- COMMAND ----------

-- create a table with LS0A to IMD lookup
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_imd USING PARQUET AS
SELECT
  DISTINCT LSOA_CODE_2011,
  DECI_IMD
FROM
  dss_corporate.english_indices_of_dep_v02
WHERE
  LSOA_CODE_2011 IN (
    SELECT
      LSOA
    FROM
      .gdppr__archive
  )
  AND LSOA_CODE_2011 IS NOT NULL
  AND IMD IS NOT NULL
  AND IMD_YEAR = '2019';
ALTER TABLE .ccu030_${dt}_imd OWNER TO 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_snomed_id", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_autism_snomed", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_autism_icd10", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_autism_readcode", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_autism_medcode", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

CREATE or replace global temp view ccu030_temp AS
select distinct CTV3_CONCEPTID as readcode_v3, SCT_CONCEPTID as snomed 
from dss_corporate.read_codes_map_ctv3_to_snomed
where SCT_CONCEPTID is not null;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_readcode_v3_snomed_lookup USING PARQUET AS
select t.*, t1.autism 
from global_temp.ccu030_temp t
inner join .ccu030_${dt}_autism_readcode t1 ON t.readcode_v3 = t1.readcode;
ALTER TABLE .ccu030_${dt}_readcode_v3_snomed_lookup OWNER TO 

-- COMMAND ----------

CREATE or replace global temp view ccu030_temp2 AS
select distinct V2_READCODE as readcode_v2, SCT_CONCEPTID as snomed 
from dss_corporate.read_codes_map_v2_to_snomed
where SCT_CONCEPTID is not null;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_readcode_v2_snomed_lookup USING PARQUET AS
select t.*, t1.autism 
from global_temp.ccu030_temp2 t
inner join .ccu030_${dt}_autism_readcode t1 ON t.readcode_v2 = t1.readcode;
ALTER TABLE .ccu030_${dt}_readcode_v2_snomed_lookup OWNER TO 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_LTC_helper_table", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_snomedct_ltc", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_mortality_rates", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_european_standard_population", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("ccu030nb_vaccine_names", 3600, {"dt": f"{dt}"})

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_sgss_covid
AS
SELECT person_id_deid, date, 
"01_Covid_positive_test" as covid_phenotype, 
"" as clinical_code, 
"" as description,
"confirmed (covid positive test)" as covid_status,
"" as code,
"SGSS" as source, date_is
FROM .ccu030_${dt}_tmp_sgss
