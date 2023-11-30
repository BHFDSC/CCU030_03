# Databricks notebook source
# MAGIC %md
# MAGIC  # COVID-19 Master Phenotype notebook
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Identifies all patients with COVID-19 related diagnosis
# MAGIC * Creates a *trajectory* table with all data points for all affected individuals
# MAGIC * Creates a *severity* table where all individuals are assigned a mutually-exclusive COVID-19 severity phenotype (mild, moderate, severe, death) based on the worst event they experience  
# MAGIC   
# MAGIC NB:
# MAGIC * start and stop dates for the phenotyping is defined in notebook `ccu030_${dt}_01_create_table_aliases`

# COMMAND ----------

# MAGIC %python
# MAGIC dt = dbutils.widgets.get("dt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load functions and input data

# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# Get the production date from the GDPPR table - set in notebook _01
# [production_date] is destructuring, expecting spark.sql to return a list of exactly one value similar to result = spark.sql...; result[0].value
[production_date] = spark.sql(f"SELECT DISTINCT ProductionDate as value from .ccu030_{dt}_tmp_gdppr  LIMIT 1").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Identify all patients with COVID19
# MAGIC 
# MAGIC Create a single table (**`ccu030_${dt}_covid_trajectories`**) that contains all COVID-19 events from all input tables (SGSS, GDPPR, HES_APC.. etc) with the following format.
# MAGIC 
# MAGIC |Column | Content |
# MAGIC |----------------|--------------------|
# MAGIC |patient_id_deid| Patient NHS Number |
# MAGIC |date | Date of event: date in GDPPR, speciment date SGSS, epistart HES |
# MAGIC |covid_phenotype | Cateogrical: Positive PCR test; Confirmed_COVID19; Suspected_COVID19; Lab confirmed incidence; Lab confirmed historic; Lab confirmed unclear; Clinically confirmed |
# MAGIC |clinical_code | Reported clinical code |
# MAGIC |description | Description of the clinical code if relevant|
# MAGIC |code | Type of code: ICD10; SNOMED |
# MAGIC |source | Source from which the data was drawn: SGSS; HES APC; Primary care |
# MAGIC |date_is | Original column name of date |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1: COVID postive and diagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pillar 2 table
# MAGIC -- Not included currently as there are some data issues! (table has been withdrawn)
# MAGIC -- The following codes are negative: 1322791000000100, 1240591000000102
# MAGIC -- The following codes are unknown:  1321691000000102, 1322821000000105
# MAGIC --- NOTE: The inclusion of only positive tests have already been done in notebook ccu030_${dt}_create_table_aliase
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_pillar2_covid
# MAGIC --AS
# MAGIC --SELECT person_id_deid, date, 
# MAGIC -- Decision to group all pillar 2 tests together as distinguishing lateral flow vs PCR not required for severity phenotyping
# MAGIC --"01_Covid_positive_test" as covid_phenotype, 
# MAGIC --TestResult as clinical_code, 
# MAGIC --CASE WHEN TestResult = '1322781000000102' THEN "Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)" 
# MAGIC --WHEN TestResult = '1240581000000104' THEN "Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)" else NULL END as description,
# MAGIC --'confirmed' as covid_status,
# MAGIC --"SNOMED" as code,
# MAGIC --source, date_is
# MAGIC --FROM .ccu030_${dt}_tmp_pillar2

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- SGSS table
# MAGIC --- all records are included as every record is a "positive test"
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_sgss_covid
# MAGIC AS
# MAGIC SELECT person_id_deid, date, 
# MAGIC "01_Covid_positive_test" as covid_phenotype, 
# MAGIC "" as clinical_code, 
# MAGIC "" as description,
# MAGIC "confirmed (covid positive test)" as covid_status,
# MAGIC "" as code,
# MAGIC "SGSS" as source, date_is
# MAGIC FROM .ccu030_${dt}_tmp_sgss

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- FS addition: create a table with everyone with a positive laboratory covid test, just the unique ID + earliest date
# MAGIC --- SGSS table
# MAGIC --- all records are included as every record is a "positive test"
# MAGIC drop table if exists .ccu030_${dt}_covid_tested;
# MAGIC create table if not exists .ccu030_${dt}_covid_tested using parquet as
# MAGIC SELECT person_id_deid, min(specimen_date) as test_date, 1 as tested
# MAGIC FROM .ccu030_${dt}_sgss
# MAGIC where person_id_deid is not null and specimen_date <= '2021-12-31' and specimen_date >= '2020-01-24'
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %sql 
# MAGIC --- GDPPR 
# MAGIC --- Only includes individuals with a COVID SNOMED CODE
# MAGIC --- SNOMED CODES are defined in: ccu030_${dt}_01_create_table_aliases
# MAGIC --- Optimisation =  /*+ BROADCAST(tab1) */ -- forces it to send a copy of the small table to each worker
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_gdppr_covid as
# MAGIC with cte_gdppr as (
# MAGIC SELECT /*+ BROADCAST(tab1) */ -- forces it to send a copy of the small table to each worker
# MAGIC tab2.person_id_deid, tab2.date, tab2.code, tab2.date_is, tab1.clinical_code, tab1.description
# MAGIC FROM  global_temp.ccu030_${dt}_snomed_codes_covid19 tab1
# MAGIC inner join .ccu030_${dt}_tmp_gdppr tab2 on tab1.clinical_code = tab2.code
# MAGIC )
# MAGIC SELECT person_id_deid, date, 
# MAGIC "01_GP_covid_diagnosis" as covid_phenotype,
# MAGIC clinical_code, description,
# MAGIC "GP_diagnosis" as covid_status, --- See SNOMED code description [FS: it used to be "" as covid_status,]
# MAGIC "SNOMED" as code, 
# MAGIC "GDPPR" as source, date_is 
# MAGIC from cte_gdppr

# COMMAND ----------

# MAGIC %sql
# MAGIC --- HES Dropped as source since 080621
# MAGIC --- HES_OP
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC 
# MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_op_covid as
# MAGIC --SELECT person_id_deid, date, 
# MAGIC --"01_GP_covid_diagnosis" as covid_phenotype,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC --(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
# MAGIC --when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
# MAGIC --"ICD10" as code,
# MAGIC --"HES OP" as source, 
# MAGIC --date_is
# MAGIC --FROM .ccu030_${dt}_tmp_op
# MAGIC --WHERE DIAG_4_CONCAT LIKE "%U071%"
# MAGIC --   OR DIAG_4_CONCAT LIKE "%U072%"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Covid Admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --- SUS - Hospitalisations
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_sus_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'suspected (U07.2)' Else '0' End) as description,
# MAGIC (case when DIAG_CONCAT LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC when DIAG_CONCAT LIKE "%U072%" THEN 'suspected (U07.2)' Else '0' End) as covid_status,
# MAGIC "ICD10" as code,
# MAGIC "SUS" as source, 
# MAGIC date_is
# MAGIC FROM .ccu030_${dt}_tmp_sus
# MAGIC WHERE DIAG_CONCAT LIKE "%U071%"
# MAGIC    OR DIAG_CONCAT LIKE "%U072%"

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - Hospitalisations
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_chess_covid_hospital as
# MAGIC SELECT person_id_deid, HospitalAdmissionDate as date,
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "HospitalAdmissionDate IS NOT null" as description,
# MAGIC "confirmed (CHESS)" as covid_status, --- it used to be "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "HospitalAdmissionDate" as date_is
# MAGIC FROM .ccu030_${dt}_tmp_chess
# MAGIC WHERE HospitalAdmissionDate IS NOT null

# COMMAND ----------

# MAGIC %sql
# MAGIC --- HES_APC
# MAGIC --- Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_apc_covid as
# MAGIC SELECT person_id_deid, date, 
# MAGIC "02_Covid_admission" as covid_phenotype,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# MAGIC (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected (U07.2)' Else '0' End) as covid_status,
# MAGIC "HES APC" as source, 
# MAGIC "ICD10" as code, date_is, SUSRECID
# MAGIC FROM .ccu030_${dt}_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Critical Care

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.1 ICU admission

# COMMAND ----------

# MAGIC %sql
# MAGIC --- CHESS - ICU
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_chess_covid_icu as
# MAGIC SELECT person_id_deid, DateAdmittedICU as date,
# MAGIC "03_ICU_admission" as covid_phenotype,
# MAGIC "" as clinical_code, 
# MAGIC "DateAdmittedICU IS NOT null" as description,
# MAGIC "confirmed (CHESS)" as covid_status, --- it used to be "" as covid_status,
# MAGIC "CHESS" as source, 
# MAGIC "" as code,
# MAGIC "DateAdmittedICU" as date_is
# MAGIC FROM .ccu030_${dt}_tmp_chess
# MAGIC WHERE DateAdmittedICU IS NOT null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HES_CC
# MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_cc_covid as
# MAGIC SELECT apc.person_id_deid, cc.date,
# MAGIC '03_ICU_admission' as covid_phenotype,
# MAGIC "" as clinical_code,
# MAGIC "id is in hes_cc table" as description,
# MAGIC "U07.1 or U07.2 (HES)" as covid_status, -- it used to be "" as covid_status,
# MAGIC "" as code,
# MAGIC 'HES CC' as source, cc.date_is, BRESSUPDAYS, ARESSUPDAYS
# MAGIC FROM .ccu030_${dt}_tmp_apc as apc
# MAGIC INNER JOIN .ccu030_${dt}_tmp_cc3 AS cc
# MAGIC ON cc.SUSRECID = apc.SUSRECID
# MAGIC WHERE cc.BESTMATCH = 1
# MAGIC AND (DIAG_4_CONCAT LIKE '%U071%' OR DIAG_4_CONCAT LIKE '%U072%')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4: Death from COVID

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Identify all individuals with a covid diagnosis as death cause
# MAGIC --- OBS: 280421 - before we got a max date of death by grouping the query on person_id_deid. I cannot get this working after adding the case bit to determine confrimed/suspected
# MAGIC ---               so multiple deaths per person might present in table if they exsist in the raw input!
# MAGIC --- CT: Re ^ this is fine as we're presenting a view of the data as it stands, including multiple events etc. further filtering will occurr downstream
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_covid_severe_death as
# MAGIC SELECT person_id_deid, death_date as date,
# MAGIC "04_Fatal_with_covid_diagnosis" as covid_phenotype,
# MAGIC '04_fatal' as covid_severity,
# MAGIC (CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'U071'
# MAGIC  WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'U071' Else 'U072' End) as clinical_code, 
# MAGIC '' as description, 
# MAGIC 'ICD10' as code, 
# MAGIC 'deaths' as source, 'REG_DATE_OF_DEATH' as date_is,
# MAGIC (CASE WHEN S_UNDERLYING_COD_ICD10 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_1 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_2 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_3 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_4 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_5 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_6 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_7 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_8 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_9 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_10 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_11 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_12 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_13 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_14 LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC  WHEN S_COD_CODE_15 LIKE "%U071%" THEN 'confirmed (U07.1)' Else 'suspected (U07.2)' End) as covid_status
# MAGIC FROM .ccu030_${dt}_tmp_deaths
# MAGIC WHERE ((S_UNDERLYING_COD_ICD10 LIKE "%U071%") OR (S_UNDERLYING_COD_ICD10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_1 LIKE "%U071%") OR (S_COD_CODE_1 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_2 LIKE "%U071%") OR (S_COD_CODE_2 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_3 LIKE "%U071%") OR (S_COD_CODE_3 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_4 LIKE "%U071%") OR (S_COD_CODE_4 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_5 LIKE "%U071%") OR (S_COD_CODE_5 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_6 LIKE "%U071%") OR (S_COD_CODE_6 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_7 LIKE "%U071%") OR (S_COD_CODE_7 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_8 LIKE "%U071%") OR (S_COD_CODE_8 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_9 LIKE "%U071%") OR (S_COD_CODE_9 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_10 LIKE "%U071%") OR (S_COD_CODE_10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_11 LIKE "%U071%") OR (S_COD_CODE_11 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_12 LIKE "%U071%") OR (S_COD_CODE_12 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_13 LIKE "%U071%") OR (S_COD_CODE_13 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_14 LIKE "%U071%") OR (S_COD_CODE_14 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_15 LIKE "%U071%") OR (S_COD_CODE_15 LIKE "%U072%"))

# COMMAND ----------

# MAGIC %md **death not from covid:**

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists .ccu030_${dt}_non_covid_death;
# MAGIC CREATE table if not exists .ccu030_${dt}_non_covid_death as
# MAGIC with cte as (
# MAGIC   select *,
# MAGIC   case when ((S_UNDERLYING_COD_ICD10 LIKE "%U071%") OR (S_UNDERLYING_COD_ICD10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_1 LIKE "%U071%") OR (S_COD_CODE_1 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_2 LIKE "%U071%") OR (S_COD_CODE_2 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_3 LIKE "%U071%") OR (S_COD_CODE_3 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_4 LIKE "%U071%") OR (S_COD_CODE_4 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_5 LIKE "%U071%") OR (S_COD_CODE_5 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_6 LIKE "%U071%") OR (S_COD_CODE_6 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_7 LIKE "%U071%") OR (S_COD_CODE_7 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_8 LIKE "%U071%") OR (S_COD_CODE_8 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_9 LIKE "%U071%") OR (S_COD_CODE_9 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_10 LIKE "%U071%") OR (S_COD_CODE_10 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_11 LIKE "%U071%") OR (S_COD_CODE_11 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_12 LIKE "%U071%") OR (S_COD_CODE_12 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_13 LIKE "%U071%") OR (S_COD_CODE_13 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_14 LIKE "%U071%") OR (S_COD_CODE_14 LIKE "%U072%")) OR
# MAGIC      ((S_COD_CODE_15 LIKE "%U071%") OR (S_COD_CODE_15 LIKE "%U072%")) then 1 else 0 end as covid_death
# MAGIC   from .ccu030_${dt}_tmp_deaths
# MAGIC )
# MAGIC SELECT person_id_deid, first(death_date) as non_covid_death_date, 1 as non_covid_death
# MAGIC FROM cte
# MAGIC WHERE covid_death != 1 AND death_date is not null
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.2 Deaths during COVID admission

# COMMAND ----------

# MAGIC %sql
# MAGIC -- APC inpatient deaths during COVID-19 admission
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_apc_covid_deaths as
# MAGIC SELECT 
# MAGIC   person_id_deid,
# MAGIC   DISDATE as date,
# MAGIC   "04_Covid_inpatient_death" as covid_phenotype,
# MAGIC   "" as clinical_code,
# MAGIC   (case when 
# MAGIC       DISMETH = 4 THEN 'DISMETH = 4 (Died)'
# MAGIC   when 
# MAGIC       DISDEST = 79 THEN 'DISDEST = 79 (Not applicable - PATIENT died or still birth)' 
# MAGIC   Else '0' End) as description,
# MAGIC   (case when 
# MAGIC       DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC   when 
# MAGIC       DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected (U07.2)' 
# MAGIC   Else '0' End) as covid_status,
# MAGIC   "" as code,
# MAGIC   "HES APC" as source, 
# MAGIC   "DISDATE" as date_is
# MAGIC FROM
# MAGIC   .ccu030_${dt}_tmp_apc
# MAGIC WHERE 
# MAGIC   (DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%")
# MAGIC AND (DISMETH = 4 -- died
# MAGIC       OR 
# MAGIC     DISDEST = 79) -- discharge destination not applicable, died or stillborn
# MAGIC     -- WARNING hard-coded study-start date
# MAGIC AND (DISDATE >= TO_DATE("20200123", "yyyyMMdd")) -- death after study start

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SUS inpatient deaths during COVID-19 admission
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_sus_covid_deaths as
# MAGIC SELECT 
# MAGIC   person_id_deid,
# MAGIC   END_DATE_HOSPITAL_PROVIDER_SPELL as date,
# MAGIC   "04_Covid_inpatient_death" as covid_phenotype,
# MAGIC   "" as clinical_code,
# MAGIC   (case when 
# MAGIC       DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 THEN 'DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 (Died)'
# MAGIC   when 
# MAGIC       DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 THEN 'DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79 (Not applicable - PATIENT died or still birth)' 
# MAGIC   Else '0' End) as description,
# MAGIC   (case when 
# MAGIC       DIAG_CONCAT LIKE "%U071%" THEN 'confirmed (U07.1)'
# MAGIC   when 
# MAGIC       DIAG_CONCAT LIKE "%U072%" THEN 'suspected (U07.2)' 
# MAGIC   Else '0' End) as covid_status,
# MAGIC   "" as code,
# MAGIC   "SUS" as source, 
# MAGIC   "END_DATE_HOSPITAL_PROVIDER_SPELL" as date_is
# MAGIC FROM
# MAGIC   .ccu030_${dt}_tmp_sus
# MAGIC WHERE 
# MAGIC   (DIAG_CONCAT LIKE "%U071%" OR DIAG_CONCAT LIKE "%U072%")
# MAGIC AND (DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL = 4 -- died
# MAGIC       OR 
# MAGIC     DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL = 79) -- discharge destination not applicable, died or stillborn
# MAGIC AND (END_DATE_HOSPITAL_PROVIDER_SPELL IS NOT NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_severe_death;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_severe_death USING parquet AS
# MAGIC SELECT * FROM global_temp.ccu030_${dt}_covid_severe_death;
# MAGIC ALTER TABLE .ccu030_${dt}_covid_severe_death OWNER TO 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trajectory table
# MAGIC Build the trajectory table from all the individual tables created in step 2. This will give a table of IDs, dates and phenotypes which are *not* exclusive. This way we can plot distributions of time intervals between events (test-admission-icu etc.) and take a **data-driven** approach to choosing thresholds to define *exclusive* severity phenotypes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Initiate a temporary trajecotry table with the SGSS data
# MAGIC drop table if exists .tmp_ccu030_${dt}_covid_trajectory_delta;
# MAGIC CREATE TABLE if not exists .tmp_ccu030_${dt}_covid_trajectory_delta USING DELTA AS
# MAGIC SELECT DISTINCT * 
# MAGIC FROM global_temp.ccu030_${dt}_sgss_covid

# COMMAND ----------

# Append each of the covid related events tables to the temporary trajectory table
for table in [f'ccu030_{dt}_gdppr_covid', f'ccu030_{dt}_apc_covid', f'ccu030_{dt}_sus_covid', f'ccu030_{dt}_chess_covid_hospital', f'ccu030_{dt}_chess_covid_icu',
              f'ccu030_{dt}_cc_covid', f'ccu030_{dt}_apc_covid_deaths', f'ccu030_{dt}_sus_covid_deaths']:
#   spark.sql(f"""REFRESH TABLE global_temp.{table}""")
  (spark.table(f'global_temp.{table}')
   .select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
   .distinct()
   .write.format("delta").mode('append')
   .saveAsTable(f'.tmp_ccu030_{dt}_covid_trajectory_delta'))

# COMMAND ----------

## Add on the fatal covid cases with diagnosis
(spark.table(f".ccu030_{dt}_covid_severe_death").select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
    .distinct()
    .write.format("delta").mode('append')
    .saveAsTable(f'.tmp_ccu030_{dt}_covid_trajectory_delta'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Identifying deaths within 28 days

# COMMAND ----------

# Identifying fatal events happing within 28 days of first covid diagnosis/event. 
from pyspark.sql.functions import *

# Get all deaths
all_fatal = spark.sql(f"""
SELECT * FROM .ccu030_{dt}_tmp_deaths
""")

# Identify earliest non fatal event from trajectory table
first_non_fatal = spark.sql(f"""
WITH list_patients_to_omit AS (SELECT person_id_deid FROM .tmp_ccu030_{dt}_covid_trajectory_delta WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis')
SELECT /*+ BROADCAST(list_patients_to_omit) */
t.person_id_deid, MIN(t.date) AS first_covid_event
FROM .tmp_ccu030_{dt}_covid_trajectory_delta as t
LEFT ANTI JOIN list_patients_to_omit ON t.PERSON_ID_DEID = list_patients_to_omit.PERSON_ID_DEID
GROUP BY t.person_id_deid
""")

# Join with death data - at this stage, not filtering by death cause
# since events with covid as the cause are already in the 
# trajectories table and are excluded in the step above.
first_non_fatal = first_non_fatal.join(all_fatal, ['person_id_deid'], how='left')
first_non_fatal = first_non_fatal.select(['person_id_deid', 'first_covid_event', 'death_date']) 

# Calculate elapsed number of days between earliest event and death date
first_non_fatal = first_non_fatal.withColumn('days_to_death', \
  when(~first_non_fatal['death_date'].isNull(), \
       datediff(first_non_fatal["death_date"], first_non_fatal['first_covid_event'])).otherwise(-1))
 
# Mark deaths within 28 days
first_non_fatal = first_non_fatal.withColumn('28d_death', \
  when((first_non_fatal['days_to_death'] >= 0) & (first_non_fatal['days_to_death'] <= 28), 1).otherwise(0))

# Merge data into main trajectory table (flag as suspected not confirmed!)
first_non_fatal.createOrReplaceGlobalTempView('first_non_fatal')
  

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Write events to trajectory table
# MAGIC --- person_id_deid, date, covid_phenotype, clinical_code, description, covid_status, code, source, date_is
# MAGIC create or replace global temp view ccu030_${dt}_covid_trajectory_final_temp AS
# MAGIC select * from .tmp_ccu030_${dt}_covid_trajectory_delta UNION ALL
# MAGIC select distinct
# MAGIC   person_id_deid, 
# MAGIC   death_date as date,
# MAGIC   '04_Fatal_without_covid_diagnosis' as covid_phenotype,
# MAGIC   '' AS clinical_code,
# MAGIC   'ONS death within 28 days' AS description,
# MAGIC   'suspected (fatal without covid diagnosis)' AS covid_status,
# MAGIC   '' AS code,
# MAGIC   'deaths' AS source,
# MAGIC   'death_date' AS date_is
# MAGIC FROM
# MAGIC   global_temp.first_non_fatal
# MAGIC WHERE
# MAGIC   28d_death = 1;
# MAGIC   

# COMMAND ----------

## Add in production Date and save as final trajectory table
traject = spark.sql(f'''SELECT * FROM global_temp.ccu030_{dt}_covid_trajectory_final_temp''')
traject = traject.withColumn('ProductionDate', lit(str(production_date.value)))
traject.createOrReplaceGlobalTempView(f'ccu030_{dt}_covid_trajectory')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_trajectory;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_trajectory USING parquet AS
# MAGIC SELECT * FROM global_temp.ccu030_${dt}_covid_trajectory;
# MAGIC ALTER TABLE .ccu030_${dt}_covid_trajectory OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM .ccu030_${dt}_covid_trajectory

# COMMAND ----------

# MAGIC %sql
# MAGIC select covid_phenotype, count(*)
# MAGIC from .ccu030_${dt}_covid_trajectory
# MAGIC group by covid_phenotype

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Check counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, source, count(DISTINCT person_id_deid) as count
# MAGIC FROM .ccu030_${dt}_covid_trajectory
# MAGIC group by covid_phenotype, source
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_phenotype, count(DISTINCT person_id_deid) as count
# MAGIC FROM .ccu030_${dt}_covid_trajectory
# MAGIC group by covid_phenotype
# MAGIC order by covid_phenotype

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT person_id_deid)
# MAGIC FROM .ccu030_${dt}_covid_trajectory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Severity table  
# MAGIC Classify all participants according to their most severe COVID-19 event into a severity phenotype (mild, moderate, severe, death), i.e. these are mutually exclusive and patients can only have one severity classification.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: Mild COVID
# MAGIC * No hosptitalisation
# MAGIC * No death within 4 weeks of first diagnosis
# MAGIC * No death ever with COVID diagnosis

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_mild;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_mild USING parquet AS
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from .ccu030_${dt}_covid_trajectory WHERE covid_phenotype IN ('02_Covid_admission', '03_ICU_treatment', '04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT /*+ BROADCAST(list_patients_to_omit) */ person_id_deid, min(date) as date, '01_not_hospitalised' as covid_severity 
# MAGIC FROM .ccu030_${dt}_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_covid_mild OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct covid_severity from .ccu030_${dt}_covid_mild

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Moderate COVID - Hospitalised
# MAGIC - Hospital admission
# MAGIC - No critical care within that admission
# MAGIC - No death within 4 weeks
# MAGIC - No death from COVID diagnosis ever

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Moderate COVID
# MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_moderate;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_moderate USING parquet AS
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from .ccu030_${dt}_covid_trajectory WHERE covid_phenotype IN ('03_ICU_admission','04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '02_hospitalised' as covid_severity 
# MAGIC FROM .ccu030_${dt}_covid_trajectory as t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE covid_phenotype = '02_Covid_admission'
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_covid_moderate OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct covid_severity from .ccu030_${dt}_covid_moderate

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4.3: Severe COVID - Hospitalised with critical care
# MAGIC Hospitalised with one of the following treatments
# MAGIC - ICU treatment 
# MAGIC - NIV and/or IMV treatment - FS: DROPPED
# MAGIC - ECMO treatment - FS: DROPPED

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FS UPDATED CODE, 31/01/22
# MAGIC -- OLD CODE IS DIRECTLY BELOW
# MAGIC --- Severe COVID
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS .ccu030_${dt}_covid_severe;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_severe USING parquet AS
# MAGIC WITH list_patients_to_omit AS (SELECT person_id_deid from .ccu030_${dt}_covid_trajectory
# MAGIC WHERE covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC SELECT person_id_deid, min(date) as date, '03_hospitalised_ICU' as covid_severity 
# MAGIC FROM .ccu030_${dt}_covid_trajectory AS t
# MAGIC LEFT ANTI JOIN list_patients_to_omit ON t.person_id_deid = list_patients_to_omit.person_id_deid
# MAGIC WHERE covid_phenotype IN ('03_ICU_admission')
# MAGIC group by person_id_deid
# MAGIC 
# MAGIC -- old code:
# MAGIC --  SELECT person_id_deid, min(date) as date, '03_hospitalised_ventilatory_support' as covid_severity 
# MAGIC --  WHERE covid_phenotype IN ('03_ECMO_treatment', '03_IMV_treatment', '03_NIV_treatment', '03_ICU_treatment')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_covid_severe OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct covid_severity from .ccu030_${dt}_covid_severe

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4: Fatal COVID
# MAGIC - Fatal Covid with confirmed or supsected Covid on death register
# MAGIC - Death within 28 days without Covid on death register

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Fatal COVID
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_covid_fatal as
# MAGIC SELECT person_id_deid, min(date) as date, '04_fatal' as covid_severity 
# MAGIC FROM .ccu030_${dt}_covid_trajectory
# MAGIC WHERE (covid_phenotype IN ('04_Fatal_with_covid_diagnosis','04_Fatal_without_covid_diagnosis', '04_Covid_inpatient_death'))
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5: Final combined severity table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_covid_severity_temp
# MAGIC as SELECT * FROM .ccu030_${dt}_covid_mild
# MAGIC UNION ALL
# MAGIC SELECT * FROM .ccu030_${dt}_covid_moderate
# MAGIC UNION ALL
# MAGIC SELECT * FROM .ccu030_${dt}_covid_severe
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.ccu030_${dt}_covid_fatal

# COMMAND ----------

severe = spark.sql(f'''SELECT * FROM global_temp.ccu030_{dt}_covid_severity_temp''')
severe = severe.withColumn('ProductionDate', lit(str(production_date.value)))
severe.createOrReplaceGlobalTempView(f'ccu030_{dt}_covid_severity')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_covid_severity as
# MAGIC SELECT t.*, t2.covid_status
# MAGIC FROM global_temp.ccu030_${dt}_covid_severity t
# MAGIC LEFT JOIN .ccu030_${dt}_covid_trajectory t2 ON t.person_id_deid = t2.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS .ccu030_${dt}_covid_severity;
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_severity2 USING parquet AS
# MAGIC SELECT person_id_deid, covid_severity , covid_status, date AS covid_date
# MAGIC FROM global_temp.ccu030_${dt}_covid_severity;
# MAGIC ALTER TABLE .ccu030_${dt}_covid_severity2 OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- covid hospitalisation [regardless of death]
# MAGIC drop table if exists .ccu030_${dt}_covid_admission;
# MAGIC CREATE table if not exists .ccu030_${dt}_covid_admission as
# MAGIC SELECT person_id_deid, min(date) as covid_admission_date, 1 as covid_admission 
# MAGIC FROM .ccu030_${dt}_covid_trajectory
# MAGIC WHERE (covid_phenotype IN ('02_Covid_admission','03_ICU_admission'))
# MAGIC group by person_id_deid
# MAGIC -- note that I am not including 04_Covid_inpatient_death - as this would capture cases of death when covid was contracted at the hospital 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_severity USING parquet AS
# MAGIC WITH cte AS
# MAGIC (
# MAGIC    SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY person_id_deid ORDER BY covid_date asc) AS rn
# MAGIC    FROM .ccu030_${dt}_covid_severity2
# MAGIC )
# MAGIC SELECT person_id_deid, covid_severity, covid_date, covid_status
# MAGIC FROM cte
# MAGIC WHERE rn = 1;
# MAGIC ALTER TABLE .ccu030_${dt}_covid_severity OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM .ccu030_${dt}_covid_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Check counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT covid_severity, count(DISTINCT person_id_deid)
# MAGIC FROM .ccu030_${dt}_covid_severity
# MAGIC group by covid_severity
# MAGIC order by covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OLD value @ 30.04.21 = 3929787
# MAGIC -- OLD value @ 04.05.21 = 3933813
# MAGIC -- OLD value @ 28.05.21 = 4287595
# MAGIC -- OLD value @ 02.06.21 = 4346793
# MAGIC -- OLD value @ 04.06.21 = 3977185
# MAGIC -- OLD value @ 15.06.21 = 3772432
# MAGIC -- OLD value @ 15.06.21 = 3992872
# MAGIC -- OLD value @ 17.08.21 = 5044357
# MAGIC SELECT count(DISTINCT person_id_deid)
# MAGIC FROM .ccu030_${dt}_covid_severity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check if there is one record per id, or multiple
# MAGIC create or replace global temp view ccu030_${dt}_temp as
# MAGIC SELECT person_id_deid AS person_id_deid2, count(*) AS freq
# MAGIC FROM .ccu030_${dt}_covid_severity
# MAGIC group by person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct freq from global_temp.ccu030_${dt}_temp
# MAGIC order by freq desc
# MAGIC -- OK, everyone has only one record

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check if all 'not hospitalised' records are covid confirmed
# MAGIC -- need to join tables
# MAGIC select t.* , t2.covid_status
# MAGIC from .ccu030_${dt}_covid_severity t
# MAGIC left join .ccu030_${dt}_covid_trajectory t2 ON t.person_id_deid = t2.person_id_deid
# MAGIC -- now 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from .ccu030_${dt}_covid_trajectory

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct covid_status, count(*)
# MAGIC from .ccu030_${dt}_covid_trajectory
# MAGIC group by covid_status

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct covid_phenotype, count(*)
# MAGIC from .ccu030_${dt}_covid_trajectory
# MAGIC where covid_status = ''
# MAGIC group by covid_phenotype

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date
# MAGIC select person_id_deid, date as covid_diagnosis_date
# MAGIC from .ccu030_${dt}_covid_trajectory
# MAGIC where (covid_phenotype == '01_Covid_positive_test' or covid_phenotype == '01_GP_covid_diagnosis') and date is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from .ccu030_${dt}_covid_diagnosis_date

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date2
# MAGIC select t.*, t2.covid_date
# MAGIC from .ccu030_${dt}_covid_diagnosis_date t
# MAGIC left join .ccu030_${dt}_covid_severity t2 ON t.person_id_deid = t2.person_id_deid

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists .ccu030_${dt}_covid_diagnosis_date3;
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date3
# MAGIC select *, 
# MAGIC datediff(covid_date, covid_diagnosis_date) AS time_temp
# MAGIC from .ccu030_${dt}_covid_diagnosis_date2

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists .ccu030_${dt}_covid_diagnosis_date4;
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date4
# MAGIC select * , 
# MAGIC case
# MAGIC when time_temp < 0 then 0
# MAGIC when time_temp > 28 then 0
# MAGIC else time_temp
# MAGIC end as time_temp2
# MAGIC from .ccu030_${dt}_covid_diagnosis_date3

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists .ccu030_${dt}_covid_diagnosis_date5;
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date5
# MAGIC WITH cte AS
# MAGIC (
# MAGIC    SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY person_id_deid ORDER BY time_temp2 DESC) AS rn
# MAGIC    FROM .ccu030_${dt}_covid_diagnosis_date4
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM cte
# MAGIC WHERE rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists .ccu030_${dt}_covid_diagnosis_date6;
# MAGIC create table if not exists .ccu030_${dt}_covid_diagnosis_date6
# MAGIC select * , 
# MAGIC case
# MAGIC when time_temp2 = '0' then null
# MAGIC else time_temp2
# MAGIC end as time
# MAGIC from .ccu030_${dt}_covid_diagnosis_date5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from .ccu030_${dt}_covid_diagnosis_date6
# MAGIC order by time desc

# COMMAND ----------


