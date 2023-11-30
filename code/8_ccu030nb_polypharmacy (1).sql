-- Databricks notebook source
-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_primary_care_meds AS 
SELECT Person_ID_DEID, ProcessingPeriodDate, PrescribedBNFCode
FROM .ccu030_${dt}_primary_care_meds2
WHERE Person_ID_DEID IS NOT NULL AND PrescribedBNFCode IS NOT NULL AND ProcessingPeriodDate >= '2019-05-27'
-- this is 240 days before the first covid death in the UK

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_primary_care_meds AS 
SELECT *
FROM global_temp.ccu030_${dt}_primary_care_meds
WHERE 
PrescribedBNFCode LIKE "01%" OR 
PrescribedBNFCode LIKE "02%" OR 
PrescribedBNFCode LIKE "03%" OR 
PrescribedBNFCode LIKE "04%" OR 
PrescribedBNFCode LIKE "05%" OR 
PrescribedBNFCode LIKE "06%" OR 
PrescribedBNFCode LIKE "07%" OR 
PrescribedBNFCode LIKE "08%" OR 
PrescribedBNFCode LIKE "09%" OR 
PrescribedBNFCode LIKE "10%" OR
PrescribedBNFCode LIKE "11%" OR
PrescribedBNFCode LIKE "12%" OR 
PrescribedBNFCode LIKE "13%" OR 
PrescribedBNFCode LIKE "14%" OR 
PrescribedBNFCode LIKE "15%"

-- COMMAND ----------

-- MAGIC %md import covid infection date and death date for each patient

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT M.*, S.covid_date, D.death_date 
FROM global_temp.ccu030_${dt}_primary_care_meds M
LEFT JOIN .ccu030_${dt}_covid_severity S ON M.Person_ID_DEID = S.person_id_deid
LEFT JOIN .ccu030_${dt}_tmp_deaths D ON M.Person_ID_DEID = D.person_id_deid

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT *, 
case
when covid_date is not null then covid_date
when covid_date is null and death_date < '2020-12-31' then death_date
else '2020-12-31'
end as mydate
FROM global_temp.ccu030_${dt}_primary_care_meds 

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT *, date_sub(mydate, 240) AS start_date, date_sub(mydate, 15) AS end_date
FROM global_temp.ccu030_${dt}_primary_care_meds 

-- COMMAND ----------

-- drop duplicates of the same medication and focus on cases within the period of interest
CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT distinct Person_ID_DEID, PrescribedBNFCode
FROM global_temp.ccu030_${dt}_primary_care_meds 
where ProcessingPeriodDate >= start_date AND ProcessingPeriodDate <= end_date

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_primary_care_meds3;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_primary_care_meds3 USING PARQUET AS
SELECT Person_ID_DEID, count(*) AS medcount
FROM global_temp.ccu030_${dt}_primary_care_meds 
GROUP BY Person_ID_DEID;
ALTER TABLE .ccu030_${dt}_primary_care_meds3 OWNER TO 

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT Person_ID_DEID, substring(PrescribedBNFCode, 1, 9) as PrescribedBNFCode2
FROM global_temp.ccu030_${dt}_primary_care_meds;

CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_primary_care_meds AS
SELECT t.*, t1.antipsychotic, t2.antidepressant, t3.anticonvulsant
FROM global_temp.ccu030_${dt}_primary_care_meds t
left join .ccu030_antipsychotics t1 on t.PrescribedBNFCode2 = t1.PrescribedBNFCode
left join .ccu030_antidepressants t2 on t.PrescribedBNFCode2 = t2.PrescribedBNFCode
left join .ccu030_anticonvulsants t3 on t.PrescribedBNFCode2 = t3.PrescribedBNFCode; 

DROP TABLE IF EXISTS .ccu030_${dt}_primary_care_meds4;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_primary_care_meds4 USING PARQUET AS
SELECT Person_ID_DEID, max(antipsychotic) AS antipsychotic, max(antidepressant) AS antidepressant, max(anticonvulsant) AS anticonvulsant
FROM global_temp.ccu030_${dt}_primary_care_meds 
GROUP BY Person_ID_DEID;
ALTER TABLE .ccu030_${dt}_primary_care_meds4 OWNER TO 
