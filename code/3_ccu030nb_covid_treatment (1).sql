-- Databricks notebook source
-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit, col, udf
-- MAGIC from functools import reduce
-- MAGIC from pyspark.sql import DataFrame
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.types import DateType

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3.2 NIV

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --- CHESS - NIV
-- MAGIC -- DROP TABLE IF EXISTS .ccu030_${dt}_covid_niv_chess;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_chess USING PARQUET AS
-- MAGIC SELECT person_id_deid, HospitalAdmissionDate as date,
-- MAGIC "NIV_treatment" as covid_phenotype,
-- MAGIC "" as clinical_code, 
-- MAGIC "Highflownasaloxygen OR NoninvasiveMechanicalventilation == Yes" as description,
-- MAGIC "" as covid_status,
-- MAGIC "CHESS" as source, 
-- MAGIC "" as code,
-- MAGIC "HospitalAdmissionDate" as date_is -- Can't be any more precise
-- MAGIC FROM .ccu030_${dt}_tmp_chess
-- MAGIC WHERE HospitalAdmissionDate IS NOT null
-- MAGIC AND (Highflownasaloxygen == "Yes" OR NoninvasiveMechanicalventilation == "Yes")

-- COMMAND ----------

ALTER TABLE .ccu030_${dt}_covid_niv_chess OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_chess2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as niv_chess
from .ccu030_${dt}_covid_niv_chess;
ALTER TABLE .ccu030_${dt}_covid_niv_chess2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- HES CC NIV
-- MAGIC -- Admissions where BRESSUPDAYS > 0 (i.e there was some BASIC respiratory support)
-- MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_niv_cc;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_cc USING PARQUET AS
-- MAGIC SELECT person_id_deid, date,
-- MAGIC 'NIV_treatment' as covid_phenotype,
-- MAGIC "" as clinical_code,
-- MAGIC "bressupdays > 0" as description,
-- MAGIC "" as covid_status,
-- MAGIC "" as code,
-- MAGIC 'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
-- MAGIC FROM .ccu030_${dt}_tmp_cc3
-- MAGIC WHERE BRESSUPDAYS > 0;
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_niv_cc OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_cc2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as niv_cc
from .ccu030_${dt}_covid_niv_cc;
ALTER TABLE .ccu030_${dt}_covid_niv_cc2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- HES APC NIV
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_niv_apc;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_apc USING PARQUET AS
-- MAGIC SELECT person_id_deid, date, 
-- MAGIC "NIV_treatment" as covid_phenotype,
-- MAGIC (case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'E85.2'
-- MAGIC when OPERTN_4_CONCAT LIKE "%E856%" THEN 'E85.6' Else '0' End) as clinical_code,
-- MAGIC (case when OPERTN_4_CONCAT LIKE "%E852%" THEN 'Non-invasive ventilation NEC'
-- MAGIC when OPERTN_4_CONCAT LIKE "%E856%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
-- MAGIC "" as covid_status,
-- MAGIC "HES APC" as source,
-- MAGIC "OPCS" as code, date_is, SUSRECID
-- MAGIC FROM .ccu030_${dt}_tmp_apc
-- MAGIC WHERE (DIAG_4_CONCAT LIKE "%U071%"
-- MAGIC    OR DIAG_4_CONCAT LIKE "%U072%")
-- MAGIC AND (OPERTN_4_CONCAT LIKE '%E852%' 
-- MAGIC       OR OPERTN_4_CONCAT LIKE '%E856%');
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_niv_apc OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_apc2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as niv_apc
from .ccu030_${dt}_covid_niv_apc;
ALTER TABLE .ccu030_${dt}_covid_niv_apc2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- SUS NIV
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_niv_sus;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_sus USING PARQUET AS
-- MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
-- MAGIC "NIV_treatment" as covid_phenotype,
-- MAGIC (case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'E85.2'
-- MAGIC when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'E85.6' Else '0' End) as clinical_code,
-- MAGIC (case when PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" THEN 'Non-invasive ventilation NEC'
-- MAGIC when PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%" THEN 'Continuous positive airway pressure' Else '0' End) as description,
-- MAGIC "" as covid_status,
-- MAGIC "SUS" as source, 
-- MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
-- MAGIC FROM .ccu030_${dt}_tmp_sus
-- MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
-- MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
-- MAGIC    (PROCEDURE_CONCAT LIKE "%E852%" OR PROCEDURE_CONCAT LIKE "%E85.2%" OR PROCEDURE_CONCAT LIKE "%E856%" OR PROCEDURE_CONCAT LIKE "%E85.6%") AND
-- MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL;
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_niv_sus OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_niv_sus2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as niv_sus
from .ccu030_${dt}_covid_niv_sus;
ALTER TABLE .ccu030_${dt}_covid_niv_sus2 OWNER TO 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3.3 IMV

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- HES CC IMV
-- MAGIC -- Admissions where ARESSUPDAYS > 0 (i.e there was some ADVANCED respiratory support)
-- MAGIC -- ID is in HES_CC AND has U071 or U072 from HES_APC 
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_imv_cc;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_cc USING PARQUET AS
-- MAGIC SELECT person_id_deid, date,
-- MAGIC 'IMV_treatment' as covid_phenotype,
-- MAGIC "" as clinical_code,
-- MAGIC "ARESSUPDAYS > 0" as description,
-- MAGIC "" as covid_status,
-- MAGIC "" as code,
-- MAGIC 'HES CC' as source, date_is, BRESSUPDAYS, ARESSUPDAYS
-- MAGIC FROM .ccu030_${dt}_tmp_cc3
-- MAGIC WHERE ARESSUPDAYS > 0;
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_imv_cc OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_cc2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as imv_cc
from .ccu030_${dt}_covid_imv_cc;
ALTER TABLE .ccu030_${dt}_covid_imv_cc2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --- CHESS - IMV
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_imv_chess;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_chess USING PARQUET AS
-- MAGIC SELECT person_id_deid, DateAdmittedICU as date,
-- MAGIC "IMV_treatment" as covid_phenotype,
-- MAGIC "" as clinical_code, 
-- MAGIC "Invasivemechanicalventilation == Yes" as description,
-- MAGIC "" as covid_status,
-- MAGIC "CHESS" as source, 
-- MAGIC "" as code,
-- MAGIC "DateAdmittedICU" as date_is -- Using ICU date as probably most of the IMV happened there, but may lose some records (250/10k)
-- MAGIC FROM .ccu030_${dt}_tmp_chess
-- MAGIC WHERE DateAdmittedICU IS NOT null
-- MAGIC AND Invasivemechanicalventilation == "Yes";
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_imv_chess OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_chess2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as imv_chess
from .ccu030_${dt}_covid_imv_chess;
ALTER TABLE .ccu030_${dt}_covid_imv_chess2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- HES APC IMV
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_imv_apc;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_apc USING PARQUET AS
-- MAGIC SELECT person_id_deid, date, 
-- MAGIC "IMV_treatment" as covid_phenotype,
-- MAGIC (case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'E85.1'
-- MAGIC when OPERTN_4_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
-- MAGIC (case when OPERTN_4_CONCAT LIKE "%E851%" THEN 'Invasive ventilation'
-- MAGIC when OPERTN_4_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
-- MAGIC "" as covid_status,
-- MAGIC "HES APC" as source, 
-- MAGIC "OPCS" as code, date_is, SUSRECID
-- MAGIC FROM .ccu030_${dt}_tmp_apc
-- MAGIC WHERE (OPERTN_4_CONCAT LIKE '%E851%' 
-- MAGIC       OR OPERTN_4_CONCAT LIKE '%X56%');
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_imv_apc OWNER TO   

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_apc2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as imv_apc
from .ccu030_${dt}_covid_imv_apc;
ALTER TABLE .ccu030_${dt}_covid_imv_apc2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- SUS IMV
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_imv_sus;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_sus USING PARQUET AS
-- MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date, PROCEDURE_CONCAT,
-- MAGIC "IMV_treatment" as covid_phenotype,
-- MAGIC (case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'E85.1'
-- MAGIC when PROCEDURE_CONCAT LIKE "%X56%" THEN 'X56' Else '0' End) as clinical_code,
-- MAGIC (case when PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" THEN 'Invasive ventilation'
-- MAGIC when PROCEDURE_CONCAT LIKE "%X56%" THEN 'Intubation of trachea' Else '0' End) as description,
-- MAGIC "" as covid_status,
-- MAGIC "SUS" as source, 
-- MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
-- MAGIC FROM .ccu030_${dt}_tmp_sus
-- MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
-- MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
-- MAGIC    (PROCEDURE_CONCAT LIKE "%E851%" OR PROCEDURE_CONCAT LIKE "%E85.1%" OR PROCEDURE_CONCAT LIKE "%X56%") AND
-- MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL;
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_imv_sus OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_imv_sus2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as imv_sus
from .ccu030_${dt}_covid_imv_sus;
ALTER TABLE .ccu030_${dt}_covid_imv_sus2 OWNER TO 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3.4 ECMO

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --- CHESS - ECMO
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_ecmo_chess;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_chess USING PARQUET AS
-- MAGIC SELECT person_id_deid, DateAdmittedICU as date,
-- MAGIC "ECMO_treatment" as covid_phenotype,
-- MAGIC "" as clinical_code, 
-- MAGIC "RespiratorySupportECMO == Yes" as description,
-- MAGIC "" as covid_status,
-- MAGIC "CHESS" as source, 
-- MAGIC "" as code,
-- MAGIC "DateAdmittedICU" as date_is -- Reasonable
-- MAGIC FROM .ccu030_${dt}_tmp_chess
-- MAGIC WHERE DateAdmittedICU IS NOT null
-- MAGIC AND RespiratorySupportECMO == "Yes";
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_ecmo_chess OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_chess2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as ecmo_chess
from .ccu030_${dt}_covid_ecmo_chess;
ALTER TABLE .ccu030_${dt}_covid_ecmo_chess2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- HES APC ECMO
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_ecmo_apc;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_apc USING PARQUET AS
-- MAGIC SELECT person_id_deid, date, 
-- MAGIC "ECMO_treatment" as covid_phenotype,
-- MAGIC "X58.1" as clinical_code,
-- MAGIC "Extracorporeal membrane oxygenation" as description,
-- MAGIC "" as covid_status,
-- MAGIC "HES APC" as source, 
-- MAGIC "OPCS" as code, date_is, SUSRECID
-- MAGIC FROM .ccu030_${dt}_tmp_apc
-- MAGIC WHERE (DIAG_4_CONCAT LIKE "%U071%"
-- MAGIC    OR DIAG_4_CONCAT LIKE "%U072%")
-- MAGIC AND OPERTN_4_CONCAT LIKE '%X581%';
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_ecmo_apc OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_apc2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as ecmo_apc
from .ccu030_${dt}_covid_ecmo_apc;
ALTER TABLE .ccu030_${dt}_covid_ecmo_apc2 OWNER TO 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- SUS ECMO
-- MAGIC DROP TABLE IF EXISTS .ccu030_${dt}_covid_ecmo_sus;
-- MAGIC CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_sus USING PARQUET AS
-- MAGIC SELECT person_id_deid, PRIMARY_PROCEDURE_DATE as date,
-- MAGIC "ECMO_treatment" as covid_phenotype,
-- MAGIC "X58.1" as clinical_code,
-- MAGIC "Extracorporeal membrane oxygenation" as description,
-- MAGIC "" as covid_status,
-- MAGIC "SUS" as source, 
-- MAGIC "OPCS" as code, "PRIMARY_PROCEDURE_DATE" as date_is
-- MAGIC FROM .ccu030_${dt}_tmp_sus
-- MAGIC WHERE (DIAG_CONCAT LIKE "%U071%"
-- MAGIC    OR DIAG_CONCAT LIKE "%U072%") AND
-- MAGIC    (PROCEDURE_CONCAT LIKE "%X58.1%" OR PROCEDURE_CONCAT LIKE "%X581%") AND
-- MAGIC    PRIMARY_PROCEDURE_DATE IS NOT NULL;
-- MAGIC ALTER TABLE .ccu030_${dt}_covid_ecmo_sus OWNER TO 

-- COMMAND ----------

-- reduce this table to one record with person_id_deid and treatment indicator:
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_covid_ecmo_sus2 USING PARQUET AS
SELECT distinct person_id_deid, '1' as ecmo_sus
from .ccu030_${dt}_covid_ecmo_sus;
ALTER TABLE .ccu030_${dt}_covid_ecmo_sus2 OWNER TO 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 Check:

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * 
-- MAGIC from .ccu030_${dt}_covid_ecmo_sus
