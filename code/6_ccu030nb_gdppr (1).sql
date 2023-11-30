-- Databricks notebook source
-- MAGIC %md **GDPPR: GPES Data for Pandemic Planning and Research**
-- MAGIC 
-- MAGIC key variable is 'CODE' (string)
-- MAGIC 
-- MAGIC it is the SNOMED code value indicating the nature of the characteristic, event or intervention recorded
-- MAGIC 
-- MAGIC check if CODE has missings (empty or needs trimming) 
-- MAGIC 
-- MAGIC select top 100 CODE from .gdppr_
-- MAGIC 
-- MAGIC            where CODE like '% %'
-- MAGIC            
-- MAGIC -- OK, no missings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

describe .ccu030_${dt}_gdppr

-- COMMAND ----------

-- added 020822:
-- creating my own skinny based on GDPPR
-- I'm reducing GDPPR to one record per group of records using 'last'
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr_temp  AS
select last(NHS_NUMBER_DEID) as NHS_NUMBER_DEID, max(YEAR_OF_BIRTH) as YEAR_OF_BIRTH, max(YEAR_OF_DEATH) as YEAR_OF_DEATH, last(DATE) as gdppr_last_date, max(YEAR_MONTH_OF_BIRTH) as YEAR_MONTH_OF_BIRTH 
FROM .ccu030_${dt}_gdppr
group by NHS_NUMBER_DEID
order by gdppr_last_date asc

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr_temp  AS
select *
from global_temp.ccu030_${dt}_gdppr_temp
WHERE YEAR_MONTH_OF_BIRTH <= '2021-12' and YEAR_OF_DEATH is null or year_of_death >= '2019'

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr_skinny_temp  AS
SELECT t1.*, t2.SEX, t2.DATE_OF_BIRTH, t2.DATE_OF_DEATH, t2.ETHNIC
FROM global_temp.ccu030_${dt}_gdppr_temp t1
left join .curr302_patient_skinny_record t2 ON t1.NHS_NUMBER_DEID = t2.NHS_NUMBER_DEID;

DROP TABLE IF EXISTS .ccu030_${dt}_gdppr_skinny;

CREATE TABLE IF NOT EXISTS .ccu030_${dt}_gdppr_skinny USING PARQUET AS 
SELECT *
FROM global_temp.ccu030_${dt}_gdppr_skinny_temp
WHERE DATE_OF_DEATH is null or date_of_death >= '2019-01-01'

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
-- select *, CAST(CODE as LONG) AS snomed
select *
    FROM .ccu030_${dt}_gdppr

-- COMMAND ----------

describe global_temp.ccu030_${dt}_gdppr

-- COMMAND ----------

-- add snomed to icd10 lookup
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
SELECT t.*, t1.icd10
  FROM global_temp.ccu030_${dt}_gdppr t
  LEFT JOIN .ccu030_${dt}_snomed_icd10_lookup t1 ON t.CODE = t1.snomed

-- COMMAND ----------

-- add ID information
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
SELECT t.*, t1.*
  FROM global_temp.ccu030_${dt}_gdppr t
  LEFT JOIN .ccu030_${dt}_id_snomed t1 ON t.CODE = t1.snomed

-- COMMAND ----------

-- add autism information
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
SELECT t.*, t1.autism AS autism1, t2.autism AS autism2, t3.autism AS autism3
  FROM global_temp.ccu030_${dt}_gdppr t
  LEFT JOIN .ccu030_${dt}_readcode_v2_snomed_lookup t1 ON t.CODE = t1.snomed
  LEFT JOIN .ccu030_${dt}_readcode_v3_snomed_lookup t2 ON t.CODE = t2.snomed
  LEFT JOIN .ccu030_${dt}_autism_snomed t3 ON t.CODE = t3.snomed

-- COMMAND ----------

-- merge autism1 and autism2 info
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
SELECT *, greatest(autism1, autism2, autism3) AS autism
  FROM global_temp.ccu030_${dt}_gdppr

-- COMMAND ----------

describe global_temp.ccu030_${dt}_gdppr

-- COMMAND ----------

-- MAGIC %md **Create a table with ID indicators:**

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_id;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_id USING PARQUET AS
SELECT NHS_NUMBER_DEID AS person_id_deid, max(on_id_register) AS on_id_register, max(id) AS id, max(id_severity) AS id_severity 
FROM global_temp.ccu030_${dt}_gdppr
group by person_id_deid;
ALTER TABLE .ccu030_${dt}_id OWNER TO 

-- COMMAND ----------

-- tabulate id
SELECT id, count(distinct(person_id_deid)) AS Freq, count(distinct(person_id_deid)) * 100.0 / sum(count(distinct(person_id_deid))) over() AS Percentage
    FROM .ccu030_${dt}_id
    group by id
    order by id asc

-- COMMAND ----------

-- tabulate on_id_register
SELECT on_id_register, count(distinct(person_id_deid)) AS Freq, count(distinct(person_id_deid)) * 100.0 / sum(count(distinct(person_id_deid))) over() AS Percentage
    FROM .ccu030_${dt}_id
    group by on_id_register
    order by on_id_register asc

-- COMMAND ----------

-- tabulate severity level
SELECT id_severity, count(distinct(person_id_deid)) AS Freq, count(distinct(person_id_deid)) * 100.0 / sum(count(distinct(person_id_deid))) over() AS Percentage
    FROM .ccu030_${dt}_id
    group by id_severity
    order by id_severity asc

-- COMMAND ----------

-- MAGIC %md **Create a table with autism indicators:**

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_autism;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_autism USING PARQUET AS
SELECT NHS_NUMBER_DEID AS person_id_deid, max(autism) AS autism
FROM global_temp.ccu030_${dt}_gdppr
group by person_id_deid;
ALTER TABLE .ccu030_${dt}_autism OWNER TO 

-- COMMAND ----------

-- MAGIC %md **add covid infection date and death date:**

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr  AS
SELECT G.*, S.covid_date, D.death_date  
  FROM global_temp.ccu030_${dt}_gdppr G
  LEFT JOIN .ccu030_${dt}_covid_severity S ON G.NHS_NUMBER_DEID = S.person_id_deid 
  LEFT JOIN .ccu030_${dt}_tmp_deaths D ON G.NHS_NUMBER_DEID = D.person_id_deid

-- COMMAND ----------

-- MAGIC %md **indicators of LTCs in GDPPR:**

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_gdppr as
select *, 
case
when covid_date is not null then covid_date
when covid_date is null and death_date < '2020-12-31' then death_date
else '2020-12-31'
end as mydate
from global_temp.ccu030_${dt}_gdppr

-- COMMAND ----------

create or replace global temp view ccu030_${dt}_gdppr_ltc_temp as
select * 
from global_temp.ccu030_${dt}_gdppr
where DATE >= date_sub(mydate, 365) AND DATE <= mydate

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr_ltc_temp  AS
SELECT G.*, S.cond_abbreviation
  FROM global_temp.ccu030_${dt}_gdppr_ltc_temp G
  LEFT JOIN .ccu030_${dt}_cam_cprdcam_mc_snomedct_final S ON G.CODE = S.conceptId

-- COMMAND ----------

-- MAGIC %md now aggregate info about each LTC
-- MAGIC 
-- MAGIC loop through LTC short name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as f
-- MAGIC 
-- MAGIC # NOTE THAT LD IS OMITTED HERE
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
-- MAGIC   'THY'
-- MAGIC ]
-- MAGIC 
-- MAGIC results = (
-- MAGIC   spark.table(f"global_temp.ccu030_{dt}_gdppr_ltc_temp")
-- MAGIC   .groupby(f.col("nhs_number_deid").alias("person_id_deid_gdppr"))
-- MAGIC   .agg(*(f.when(f.array_contains(f.collect_set(f.col("cond_abbreviation")), item), 1).otherwise(0).alias(f"{item}_gdppr")
-- MAGIC          for item in items))
-- MAGIC )
-- MAGIC 
-- MAGIC mycols = [col for col in results.columns if '_gdppr' in col]
-- MAGIC 
-- MAGIC results = results.select(mycols)
-- MAGIC 
-- MAGIC results.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_gdppr_ltc_final8")

-- COMMAND ----------

-- MAGIC %md **Demographics:**
-- MAGIC 
-- MAGIC - sex, ethnicity and DOB are already in the skinny table
-- MAGIC - so only LSOA/IMD need attention

-- COMMAND ----------

-- LSOA
-- people can change areas, so it's not like sex or ethnicity
-- we want the latest non-missing record before the infection date, if there is one
-- or the first record after infection date, if there is no earlier record

CREATE or replace global temp view ccu030_${dt}_gdppr_lsoa  AS
SELECT NHS_NUMBER_DEID AS person_id_deid, LSOA, DATE, covid_date, (CASE WHEN DATE <= covid_date THEN 1 ELSE 0 END) AS before, ROW_NUMBER() OVER(PARTITION BY NHS_NUMBER_DEID ORDER BY DATE desc) AS date_desc
FROM global_temp.ccu030_${dt}_gdppr
WHERE LSOA IS NOT NULL

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_gdppr_lsoa  AS 
SELECT *, ROW_NUMBER() OVER(PARTITION BY person_id_deid ORDER BY before desc, (CASE WHEN before=1 THEN date_desc ELSE -date_desc END)) rn
FROM global_temp.ccu030_${dt}_gdppr_lsoa

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_gdppr_lsoa2;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_gdppr_lsoa2 USING PARQUET AS 
SELECT person_id_deid, LSOA
FROM global_temp.ccu030_${dt}_gdppr_lsoa
WHERE rn = '1'
-- all records are unique

-- COMMAND ----------

ALTER TABLE .ccu030_${dt}_gdppr_lsoa2 OWNER TO 

-- COMMAND ----------

-- MAGIC %md **add IMD:**

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_gdppr_lsoa_imd;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_gdppr_lsoa_imd USING PARQUET AS 
SELECT t1.person_id_deid, t1.LSOA, t2.DECI_IMD
FROM .ccu030_${dt}_gdppr_lsoa2 t1
left join .ccu030_${dt}_imd t2 ON t1.LSOA = t2.LSOA_CODE_2011
-- all records are unique

-- COMMAND ----------


