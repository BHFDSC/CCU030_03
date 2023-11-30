-- Databricks notebook source
-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")
-- MAGIC print(dt)

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_patient_skinny_record_enhanced AS
SELECT *, 
(datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), to_date(DATE_OF_BIRTH, "yyyy-MM-dd"))) / 365.25 AS age_at_death, 
(datediff(covid_date, to_date(DATE_OF_BIRTH, "yyyy-MM-dd"))) / 365.25 AS age_at_covid, 
CASE WHEN to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2020-01-01' THEN 1 ELSE 0 END AS death_after_1_1_20, 
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2019-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2019-12-31') THEN 1 ELSE 0 END AS death_in_2019,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2020-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2020-12-31') THEN 1 ELSE 0 END AS death_in_2020,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2021-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2021-12-31') THEN 1 ELSE 0 END AS death_in_2021,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2018-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2018-12-31') THEN 1 ELSE 0 END AS death_in_2018,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2017-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2017-12-31') THEN 1 ELSE 0 END AS death_in_2017,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2016-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2016-12-31') THEN 1 ELSE 0 END AS death_in_2016,
CASE WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2015-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2015-12-31') THEN 1 ELSE 0 END AS death_in_2015,
CASE 
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2019-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2019-12-31') THEN 2019
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2020-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2020-12-31') THEN 2020
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2021-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2021-12-31') THEN 2021
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2018-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2018-12-31') THEN 2018
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2017-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2017-12-31') THEN 2017
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2016-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2016-12-31') THEN 2016
WHEN (to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2015-01-01' AND to_date(DATE_OF_DEATH, "yyyy-MM-dd") <= '2015-12-31') THEN 2015 
ELSE null 
END AS yod,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2018-12-31' THEN 1 ELSE 0 END AS born_before_1_1_19,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2019-12-31' THEN 1 ELSE 0 END AS born_before_1_1_20,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2020-12-31' THEN 1 ELSE 0 END AS born_before_1_1_21,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2018-12-31' THEN 1 ELSE 0 END AS born_before_1_1_18,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2017-12-31' THEN 1 ELSE 0 END AS born_before_1_1_17,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2016-12-31' THEN 1 ELSE 0 END AS born_before_1_1_16,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") <= '2015-12-31' THEN 1 ELSE 0 END AS born_before_1_1_15,
CASE WHEN DATE_OF_BIRTH IS NULL THEN 1 ELSE 0 END AS dob_missing,
CASE WHEN to_date(DATE_OF_BIRTH, "yyyy-MM-dd") >= '2002-01-01' THEN 1 ELSE 0 END AS child_on_1_1_20,
(datediff('2020-01-01', to_date(DATE_OF_BIRTH, "yyyy-MM-dd"))) / 365.25 AS age_on_1_1_20,
(datediff('2021-01-01', to_date(DATE_OF_BIRTH, "yyyy-MM-dd"))) / 365.25 AS age_on_1_1_21,
CASE WHEN covid_date <= '2020-12-31' THEN 1 ELSE 0 END AS covid_in_2020, 
CASE WHEN covid_date >= '2021-01-01' AND covid_date <= '2021-12-31' THEN 1 ELSE 0 END AS covid_in_2021,
CASE WHEN covid_date >= '2022-01-01' AND covid_date <= '2022-12-31' THEN 1 ELSE 0 END AS covid_in_2022,
case 
when covid_severity == '04_fatal' then 1
when covid_severity != '04_fatal' and covid_date is not null then 0
else null 
end as covid_death,
greatest(ALC_op, ALC_apc, ALC_gdppr) AS ALC,
greatest(AB_op, AB_apc, AB_gdppr) AS AB,
greatest(ANX_op, ANX_apc, ANX_gdppr) AS ANX,
greatest(AST_op, AST_apc, AST_gdppr) AS AST,
greatest(ATR_op, ATR_apc, ATR_gdppr) AS ATR,
greatest(BLI_op, BLI_apc, BLI_gdppr) AS BLI,
greatest(BRO_op, BRO_apc, BRO_gdppr) AS BRO,
greatest(CAN_op, CAN_apc, CAN_gdppr) AS CAN,
greatest(CHD_op, CHD_apc, CHD_gdppr) AS CHD,
greatest(CKD_op, CKD_apc, CKD_gdppr) AS CKD,
greatest(CLD_op, CLD_apc, CLD_gdppr) AS CLD,
-- greatest(CSP_op, CSP_apc, CSP_gdppr) AS CSP, -- deactivated because we actually don't have these codes
greatest(COPD_op, COPD_apc, COPD_gdppr) AS COPD,
greatest(DEM_op, DEM_apc, DEM_gdppr) AS DEM, 
greatest(DEP_op, DEP_apc, DEP_gdppr) AS DEP,
greatest(DIA_op, DIA_apc, DIA_gdppr) AS DIA,
greatest(DIV_op, DIV_apc, DIV_gdppr) AS DIV,
greatest(EPI_op, EPI_apc, EPI_gdppr) AS EPI,
greatest(HF_op, HF_apc, HF_gdppr) AS HF,
greatest(HL_op, HL_apc, HL_gdppr) AS HL,
greatest(HYP_op, HYP_apc, HYP_gdppr) AS HYP,
greatest(IBD_op, IBD_apc, IBD_gdppr) AS IBD,
greatest(IBS_op, IBS_apc, IBS_gdppr) AS IBS,
greatest(MIG_op, MIG_apc, MIG_gdppr) AS MIG,
greatest(MS_op, MS_apc, MS_gdppr) AS MS,
greatest(PUD_op, PUD_apc, PUD_gdppr) AS PUD,
-- greatest(PNC_op, PNC_apc, PNC_gdppr) AS PNC, -- deactivated because we actually don't have these codes
greatest(PRK_op, PRK_apc, PRK_gdppr) AS PRK,
greatest(PSD_op, PSD_apc, PSD_gdppr) AS PSD,
greatest(PSM_op, PSM_apc, PSM_gdppr) AS PSM,
greatest(PSO_op, PSO_apc, PSO_gdppr) AS PSO,
greatest(PVD_op, PVD_apc, PVD_gdppr) AS PVD,
greatest(RHE_op, RHE_apc, RHE_gdppr) AS RHE,
greatest(SCZ_op, SCZ_apc, SCZ_gdppr) AS SCZ,
greatest(SIN_op, SIN_apc, SIN_gdppr) AS SIN,
greatest(STR_op, STR_apc, STR_gdppr) AS STR,
greatest(THY_op, THY_apc, THY_gdppr) AS THY,
case when vaccine_name == "astrazeneca" then 1 else 0 end as astrazeneca,
case when vaccine_name == "pfizer" then 1 else 0 end as pfizer,
case when vaccine_name == "moderna" then 1 else 0 end as moderna,
case when vaccine_name is not null and vaccine_name != "astrazeneca" and vaccine_name != "pfizer" and vaccine_name != "moderna" THEN 1 else 0 end as other_vaccine, 
case when N_DOSES is null then "none"
when N_DOSES = 1 and vaccine_name == "astrazeneca" then "1_astrazeneca"
when N_DOSES = 2 and vaccine_name == "astrazeneca" then "2_astrazeneca"
when N_DOSES = 1 and vaccine_name == "pfizer" then "1_pfizer"
when N_DOSES = 2 and vaccine_name == "pfizer" then "2_pfizer"
when N_DOSES = 1 and vaccine_name == "moderna" then "1_moderna"
when N_DOSES = 2 and vaccine_name == "moderna" then "2_moderna"
else "none" end as vaccination,
case when test_date >= '2020-01-24' and test_date <= '2021-12-31' then test_date else null end as test_date2
  FROM .ccu030_${dt}_patient_skinny_record_enhanced18
  WHERE DATE_OF_DEATH IS NULL OR to_date(DATE_OF_DEATH, "yyyy-MM-dd") >= '2020-01-01' 

-- COMMAND ----------

Select nhs_number_deid, count(*) as freq
From .ccu030_${dt}_patient_skinny_record_enhanced18
Group by nhs_number_deid
Order by freq desc

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_patient_skinny_record_enhanced AS
SELECT *, 
case 
when datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) >= 0 and datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) <= 28 and covid_death = 1 then datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) 
when datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) >= 29 then 28
when DATE_OF_DEATH is null and test_date2 is not null then 28
else null end as days_to_death,
-- else null end as days_to_censoring,
case when datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) >= 0 and datediff(to_date(DATE_OF_DEATH, "yyyy-MM-dd"), test_date2) <= 28 then 1 else 0 end as covid_death2,
case 
when datediff(covid_admission_date, test_date2) > 0 and datediff(covid_admission_date, test_date2) <= 28 then datediff(covid_admission_date, test_date2) 
when datediff(covid_admission_date, test_date2) >= 29 then 28
when covid_admission_date is null and test_date2 is not null then 28
else null end as days_to_admission,
case when datediff(covid_admission_date, test_date2) > 0 and datediff(covid_admission_date, test_date2) <= 28 then 1 else 0 end as covid_admission2,
least(covid_admission_date, to_date(DATE_OF_DEATH, "yyyy-MM-dd")) as effective_date,
case when covid_death == 1 or covid_admission == 1 then 1 else 0 end as effective_outcome
from global_temp.ccu030_${dt}_patient_skinny_record_enhanced 

-- COMMAND ----------

CREATE or replace global temp view ccu030_${dt}_patient_skinny_record_enhanced AS
SELECT *, 
case 
when datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) >= 0 and datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) <= 28 then datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) 
when datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) >= 29 then 28
when effective_date is null and test_date2 is not null then 28
else null end as days_to_censoring2,
case 
when datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) <= 28 then datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) 
when datediff(to_date(effective_date, "yyyy-MM-dd"), test_date2) >= 29 then 28
when effective_date is null and test_date2 is not null then 28
else null end as days_to_censoring
from global_temp.ccu030_${dt}_patient_skinny_record_enhanced 

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced21;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced21 USING PARQUET AS
select covid_death, non_covid_death, 
  age_on_1_1_20,
  death_in_2020, death_in_2021, 
  N_DOSES,
  vaccination,
  SEX, ethnic_group,
  id, id_severity, on_id_register, 
  autism,
  autism_no_id,
  medcount, DECI_IMD,
  ALC+AB+ANX+AST+ATR+BLI+BRO+CAN+CHD+CKD+CLD+COPD+DEM+DEP+DIA+DIV+EPI+HF+HL+HYP+IBD+IBS+MIG+MS+PUD+PRK+PSD+PSM+PSO+PVD+RHE+SCZ+SIN+STR+THY as ltc_count,
greatest(AST, ATR, BLI, BRO, CAN, CHD, CKD, CLD, COPD, DEM, DIA, DIV, EPI, HF, HL, HYP, IBD, IBS, MIG, MS, PUD, PRK, PSD, PSO, PVD, RHE, SIN, STR, THY) as physical_ltc,
antipsychotic, antidepressant, anticonvulsant
  FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
  where age_on_1_1_20 >= '0' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = spark.table(f'.ccu030_{dt}_patient_skinny_record_enhanced21')
-- MAGIC df = df.groupBy(df.columns).count()
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable(f'.ccu030_{dt}_patient_skinny_record_enhanced21_fre')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced23;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced23 USING PARQUET AS
select covid_death, non_covid_death, 
  age_on_1_1_21,
  death_in_2021, 
  N_DOSES,
  vaccination,
  SEX, ethnic_group,
  id, id_severity, on_id_register, 
  autism,
  autism_no_id,
  medcount, DECI_IMD,
  ALC+AB+ANX+AST+ATR+BLI+BRO+CAN+CHD+CKD+CLD+COPD+DEM+DEP+DIA+DIV+EPI+HF+HL+HYP+IBD+IBS+MIG+MS+PUD+PRK+PSD+PSM+PSO+PVD+RHE+SCZ+SIN+STR+THY as ltc_count,
greatest(AST, ATR, BLI, BRO, CAN, CHD, CKD, CLD, COPD, DEM, DIA, DIV, EPI, HF, HL, HYP, IBD, IBS, MIG, MS, PUD, PRK, PSD, PSO, PVD, RHE, SIN, STR, THY) as physical_ltc,
antipsychotic, antidepressant, anticonvulsant
  FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
  where age_on_1_1_21 >= '0' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = spark.table(f'.ccu030_{dt}_patient_skinny_record_enhanced23')
-- MAGIC df = df.groupBy(df.columns).count()
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable(f'.ccu030_{dt}_patient_skinny_record_enhanced23_fre')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19 USING PARQUET AS
SELECT 
-- age_at_death, 
-- death_after_1_1_20, 
death_in_2020, 
death_in_2021, 
-- DEWY'S LINE 
-- born_before_1_1_20, born_before_1_1_21, dob_missing, -- DEWY'S LINE 
-- child_on_1_1_20, age_on_1_1_20,  -- DEWY'S LINE 
SEX, ethnic_group,
-- ETHNIC, ethnicity_description,
covid_severity, 
-- covid_status, 
N_DOSES, 
astrazeneca, pfizer, moderna, vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
age_at_covid,
-- (CASE 
-- WHEN age_at_covid < 18 THEN 'under 18'
-- WHEN age_at_covid >= 18 AND age_at_covid < 25 THEN '18-24'
-- WHEN age_at_covid >= 25 AND age_at_covid < 35 THEN '25-34'
-- WHEN age_at_covid >= 35 AND age_at_covid < 45 THEN '35-44'
-- WHEN age_at_covid >= 45 AND age_at_covid < 55 THEN '45-54'
-- WHEN age_at_covid >= 55 AND age_at_covid < 65 THEN '55-64'
-- WHEN age_at_covid >= 65 AND age_at_covid < 75 THEN '65-74'
-- WHEN age_at_covid >= 75 THEN '75+'
-- ELSE null
-- END) age_band_at_covid,
deci_imd,
greatest(niv_chess, niv_apc, niv_cc, niv_sus) as niv,
greatest(imv_chess, imv_apc, imv_cc, imv_sus) as imv,
greatest(ecmo_chess, ecmo_apc, ecmo_sus) as ecmo,
covid_in_2020, covid_in_2021, 
-- covid_in_2022,
-- covid_date,
-- yod,
-- time,
covid_death,
tested, test_date2,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY,
antipsychotic, antidepressant, anticonvulsant
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
WHERE covid_severity IS NOT NULL AND age_at_covid is not null and covid_date <= '2021-12-31' and covid_date >= '2020-01-24'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # reduce that dataset to frequencies
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = spark.table(f'.ccu030_{dt}_patient_skinny_record_enhanced19')
-- MAGIC df = df.groupBy(df.columns).count()
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable(f'.ccu030_{dt}_patient_skinny_record_enhanced19_fre')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced24;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced24 USING PARQUET AS
SELECT 
case when covid_date >= '2020-01-24' and covid_date <= '2020-01-31' then 'Jan 2020'
when covid_date >= '2020-02-01' and covid_date <= '2020-02-31' then 'Feb 2020'
when covid_date >= '2020-03-01' and covid_date <= '2020-03-31' then 'Mar 2020'
when covid_date >= '2020-04-01' and covid_date <= '2020-04-31' then 'Apr 2020'
when covid_date >= '2020-05-01' and covid_date <= '2020-05-31' then 'May 2020'
when covid_date >= '2020-06-01' and covid_date <= '2020-06-31' then 'Jun 2020'
when covid_date >= '2020-07-01' and covid_date <= '2020-07-31' then 'Jul 2020'
when covid_date >= '2020-08-01' and covid_date <= '2020-08-31' then 'Aug 2020'
when covid_date >= '2020-09-01' and covid_date <= '2020-09-31' then 'Sep 2020'
when covid_date >= '2020-10-01' and covid_date <= '2020-10-31' then 'Oct 2020'
when covid_date >= '2020-11-01' and covid_date <= '2020-11-31' then 'Nov 2020'
when covid_date >= '2020-12-01' and covid_date <= '2020-12-31' then 'Dec 2020'
when covid_date >= '2021-01-01' and covid_date <= '2021-01-31' then 'Jan 2021'
when covid_date >= '2021-02-01' and covid_date <= '2021-02-31' then 'Feb 2021'
when covid_date >= '2021-03-01' and covid_date <= '2021-03-31' then 'Mar 2021'
when covid_date >= '2021-04-01' and covid_date <= '2021-04-31' then 'Apr 2021'
when covid_date >= '2021-05-01' and covid_date <= '2021-05-31' then 'May 2021'
when covid_date >= '2021-06-01' and covid_date <= '2021-06-31' then 'Jun 2021'
when covid_date >= '2021-07-01' and covid_date <= '2021-07-31' then 'Jul 2021'
when covid_date >= '2021-08-01' and covid_date <= '2021-08-31' then 'Aug 2021'
when covid_date >= '2021-09-01' and covid_date <= '2021-09-31' then 'Sep 2021'
when covid_date >= '2021-10-01' and covid_date <= '2021-10-31' then 'Oct 2021'
when covid_date >= '2021-11-01' and covid_date <= '2021-11-31' then 'Nov 2021'
when covid_date >= '2021-12-01' and covid_date <= '2021-12-31' then 'Dec 2021'
else 'null' end as covid_month,
case when covid_date >= '2020-01-24' and covid_date <= '2020-03-31' then 'Q1 2020'
when covid_date >= '2020-04-01' and covid_date <= '2020-06-31' then 'Q2 2020'
when covid_date >= '2020-07-01' and covid_date <= '2020-09-31' then 'Q3 2020'
when covid_date >= '2020-10-01' and covid_date <= '2020-12-31' then 'Q4 2020'
when covid_date >= '2021-01-01' and covid_date <= '2021-03-31' then 'Q1 2021'
when covid_date >= '2021-04-01' and covid_date <= '2021-06-31' then 'Q2 2021'
when covid_date >= '2021-07-01' and covid_date <= '2021-09-31' then 'Q3 2021'
when covid_date >= '2021-10-01' and covid_date <= '2021-12-31' then 'Q4 2021'
else 'null' end as covid_quarter,
case when covid_date >= '2020-01-24' and covid_date <= '2020-06-31' then 'H1 2020'
when covid_date >= '2020-07-01' and covid_date <= '2020-12-31' then 'H2 2020'
when covid_date >= '2021-01-01' and covid_date <= '2021-06-31' then 'H1 2021'
when covid_date >= '2021-07-01' and covid_date <= '2021-12-31' then 'H2 2021'
else 'null' end as covid_6_months,
case when covid_death = 1 and death_in_2020 then 1 else 0 end as covid_death_in_2020, 
case when covid_death = 1 and death_in_2021 then 1 else 0 end as covid_death_in_2021,  
covid_severity, 
id, 
covid_in_2020, covid_in_2021, 
covid_death
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
WHERE covid_date >= '2020-01-24' and covid_date <= '2021-12-31'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # reduce that dataset to frequencies
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = spark.table(f'.ccu030_{dt}_patient_skinny_record_enhanced24')
-- MAGIC df = df.groupBy(df.columns).count()
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable(f'.ccu030_{dt}_patient_skinny_record_enhanced24_fre')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced20;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced20 USING PARQUET AS
SELECT 
age_on_1_1_20,  
SEX, ethnic_group,
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
greatest(niv_chess, niv_apc, niv_cc, niv_sus) as niv,
greatest(imv_chess, imv_apc, imv_cc, imv_sus) as imv,
greatest(ecmo_chess, ecmo_apc, ecmo_sus) as ecmo,
(datediff(covid_date, to_date("2020-01-01"))) AS time_to_covid, 
covid_death,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= '0' and (death_in_2020 == '0' or (death_in_2020 == '1' and covid_death == '1'))

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced40;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced40 USING PARQUET AS
SELECT 
age_on_1_1_20,  
SEX, ethnic_group,
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
(datediff(covid_date, to_date("2020-01-01"))) AS time_to_covid, 
covid_death,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= '0' and 
( 
(death_in_2020 == '0' and death_in_2021 == '0') 
or 
(death_in_2020 == '1' and covid_death == '1')
or 
(death_in_2021 == '1' and covid_death == '1')
)

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced42;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced42 USING PARQUET AS
SELECT 
age_on_1_1_20,  
SEX, ethnic_group,
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_censoring,
covid_death,
covid_death2,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY,
astrazeneca, pfizer, moderna
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= '0' and days_to_censoring is not null

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced44;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced44 USING PARQUET AS
SELECT 
age_on_1_1_20,  
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_censoring,
covid_death,
covid_death2,
ALC + AB + ANX + AST + ATR + BLI + BRO + CAN + CHD + CKD + CLD + COPD + DEM + DEP + DIA + DIV + EPI + HF + HL + HYP + IBD + IBS + MIG + MS + PUD + PRK + PSD + PSM + PSO + PVD + RHE + SCZ + SIN + STR + THY as ltc_count,
astrazeneca, pfizer, moderna,
case when covid_severity = "01_not_hospitalised" or covid_severity is null  THEN 0 ELSE 1 END AS outcome,
case when covid_death2 == '1' then 1 else 0 end as covid_death_20_21, 
case when SEX = '2' THEN 1 
when SEX = '1' THEN 0
ELSE null END AS female,
case when ethnic_group = 'Asian or Asian British' then 1
when ethnic_group is null then null
ELSE 0 END AS asian,
case when ethnic_group = 'Black or Black British' then 1
when ethnic_group is null then null
ELSE 0 END AS black,
case when ethnic_group = 'Mixed' then 1
when ethnic_group is null then null
ELSE 0 END AS mixed,
case when ethnic_group = 'Other Ethnic Groups' then 1
ELSE 0 END AS other_ethnicity,
-- case when ethnic_group is null then 1 else 0 end as ethnicity_missing,
-- case when deci_imd is null then 1 else 0 end as imd_missing, 
case
when medcount <= 10 then medcount
when medcount > 10 then 10
else 0 
end as medcount2
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= 18 and days_to_censoring is not null and (SEX == '1' or SEX == '2')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced45;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced45 USING PARQUET AS
SELECT 
age_on_1_1_20,  
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_admission,
covid_admission,
covid_admission2,
ALC + AB + ANX + AST + ATR + BLI + BRO + CAN + CHD + CKD + CLD + COPD + DEM + DEP + DIA + DIV + EPI + HF + HL + HYP + IBD + IBS + MIG + MS + PUD + PRK + PSD + PSM + PSO + PVD + RHE + SCZ + SIN + STR + THY as ltc_count,
astrazeneca, pfizer, moderna,
case when covid_severity = "01_not_hospitalised" or covid_severity is null  THEN 0 ELSE 1 END AS outcome,
case when covid_death2 == '1' then 1 else 0 end as covid_death_20_21, 
case when SEX = '2' THEN 1 
when SEX = '1' THEN 0
ELSE null END AS female,
case when ethnic_group = 'Asian or Asian British' then 1
when ethnic_group is null then null
ELSE 0 END AS asian,
case when ethnic_group = 'Black or Black British' then 1
when ethnic_group is null then null
ELSE 0 END AS black,
case when ethnic_group = 'Mixed' then 1
when ethnic_group is null then null
ELSE 0 END AS mixed,
case when ethnic_group = 'Other Ethnic Groups' then 1
ELSE 0 END AS other_ethnicity,
-- case when ethnic_group is null then 1 else 0 end as ethnicity_missing,
-- case when deci_imd is null then 1 else 0 end as imd_missing, 
case
when medcount <= 10 then medcount
when medcount > 10 then 10
else 0 
end as medcount2
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= 18 and days_to_admission is not null and (SEX == '1' or SEX == '2')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced43;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced43 USING PARQUET AS
SELECT 
age_on_1_1_20,  
SEX, ethnic_group,
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_admission,
covid_admission,
covid_admission2,
covid_death,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY,
astrazeneca, pfizer, moderna
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= '0' and days_to_admission is not null

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced46;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced46 USING PARQUET AS
SELECT 
age_on_1_1_20,  
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_censoring2,
effective_outcome,
ALC + AB + ANX + AST + ATR + BLI + BRO + CAN + CHD + CKD + CLD + COPD + DEM + DEP + DIA + DIV + EPI + HF + HL + HYP + IBD + IBS + MIG + MS + PUD + PRK + PSD + PSM + PSO + PVD + RHE + SCZ + SIN + STR + THY as ltc_count,
astrazeneca, pfizer, moderna,
case when covid_severity = "01_not_hospitalised" or covid_severity is null  THEN 0 ELSE 1 END AS outcome,
case when covid_death2 == '1' then 1 else 0 end as covid_death_20_21, 
case when SEX = '2' THEN 1 
when SEX = '1' THEN 0
ELSE null END AS female,
case when ethnic_group = 'Asian or Asian British' then 1
when ethnic_group is null then null
ELSE 0 END AS asian,
case when ethnic_group = 'Black or Black British' then 1
when ethnic_group is null then null
ELSE 0 END AS black,
case when ethnic_group = 'Mixed' then 1
when ethnic_group is null then null
ELSE 0 END AS mixed,
case when ethnic_group = 'Other Ethnic Groups' then 1
ELSE 0 END AS other_ethnicity,
-- case when ethnic_group is null then 1 else 0 end as ethnicity_missing,
-- case when deci_imd is null then 1 else 0 end as imd_missing, 
case
when medcount <= 10 then medcount
when medcount > 10 then 10
else 0 
end as medcount2,
antipsychotic, antidepressant, anticonvulsant
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= 18 and days_to_censoring2 is not null and (SEX == '1' or SEX == '2')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced47;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced47 USING PARQUET AS
SELECT 
age_on_1_1_20,  
covid_severity, 
N_DOSES,
vaccination,
id, id_severity, on_id_register,
autism,
autism_no_id,
medcount,
deci_imd,
days_to_death,
days_to_censoring,
days_to_censoring2,
effective_outcome,
ALC + AB + ANX + AST + ATR + BLI + BRO + CAN + CHD + CKD + CLD + COPD + DEM + DEP + DIA + DIV + EPI + HF + HL + HYP + IBD + IBS + MIG + MS + PUD + PRK + PSD + PSM + PSO + PVD + RHE + SCZ + SIN + STR + THY as ltc_count,
astrazeneca, pfizer, moderna,
case when covid_severity = "01_not_hospitalised" or covid_severity is null  THEN 0 ELSE 1 END AS outcome,
case when covid_death2 == '1' then 1 else 0 end as covid_death_20_21, 
case when SEX = '2' THEN 1 
when SEX = '1' THEN 0
ELSE null END AS female,
case when ethnic_group = 'Asian or Asian British' then 1
when ethnic_group is null then null
ELSE 0 END AS asian,
case when ethnic_group = 'Black or Black British' then 1
when ethnic_group is null then null
ELSE 0 END AS black,
case when ethnic_group = 'Mixed' then 1
when ethnic_group is null then null
ELSE 0 END AS mixed,
case when ethnic_group = 'Other Ethnic Groups' then 1
ELSE 0 END AS other_ethnicity,
-- case when ethnic_group is null then 1 else 0 end as ethnicity_missing,
-- case when deci_imd is null then 1 else 0 end as imd_missing, 
case
when medcount <= 10 then medcount
when medcount > 10 then 10
else 0 
end as medcount2,
case 
when days_to_censoring <= 0 or (effective_date is not null and test_date2 is null) then 1
when effective_date is not null and test_date2 is not null and days_to_censoring < 28 then 0 
else null
end as flag
FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
where age_on_1_1_20 >= 18 and age_on_1_1_20 <= 100 and (days_to_censoring < 0 or (effective_date is not null and test_date2 is null)) or (effective_date is not null and test_date2 is not null and days_to_censoring < 28)

-- COMMAND ----------

-- as 21 but with individual LTCs

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced22;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced22 USING PARQUET AS
select age_on_1_1_20,
  death_in_2020, death_in_2021, 
  SEX, ethnic_group,
  id, id_severity, on_id_register, 
  autism,
  autism_no_id,
  medcount, DECI_IMD,
  ALC+AB+ANX+AST+ATR+BLI+BRO+CAN+CHD+CKD+CLD+COPD+DEM+DEP+DIA+DIV+EPI+HF+HL+HYP+IBD+IBS+MIG+MS+PUD+PRK+PSD+PSM+PSO+PVD+RHE+SCZ+SIN+STR+THY as ltc_count,
greatest(AST, ATR, BLI, BRO, CAN, CHD, CKD, CLD, COPD, DEM, DIA, DIV, EPI, HF, HL, HYP, IBD, IBS, MIG, MS, PUD, PRK, PSD, PSO, PVD, RHE, SIN, STR, THY) as physical_ltc,
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY,
antipsychotic, antidepressant, anticonvulsant
  FROM global_temp.ccu030_${dt}_patient_skinny_record_enhanced
  where age_on_1_1_20 >= '0' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # reduce it to frequencies
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = spark.table(f'.ccu030_{dt}_patient_skinny_record_enhanced22')
-- MAGIC df = df.groupBy(df.columns).count()
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable(f'.ccu030_{dt}_patient_skinny_record_enhanced22_fre')

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced18_pca;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced18_pca USING PARQUET AS
SELECT 
greatest(ALC_op, ALC_apc, ALC_gdppr) AS ALC,
greatest(AB_op, AB_apc, AB_gdppr) AS AB,
greatest(ANX_op, ANX_apc, ANX_gdppr) AS ANX,
greatest(AST_op, AST_apc, AST_gdppr) AS AST,
greatest(ATR_op, ATR_apc, ATR_gdppr) AS ATR,
greatest(BLI_op, BLI_apc, BLI_gdppr) AS BLI,
greatest(BRO_op, BRO_apc, BRO_gdppr) AS BRO,
greatest(CAN_op, CAN_apc, CAN_gdppr) AS CAN,
greatest(CHD_op, CHD_apc, CHD_gdppr) AS CHD,
greatest(CKD_op, CKD_apc, CKD_gdppr) AS CKD,
greatest(CLD_op, CLD_apc, CLD_gdppr) AS CLD,
-- greatest(CSP_op, CSP_apc, CSP_gdppr) AS CSP, -- deactivated because we actually don't have these codes
greatest(COPD_op, COPD_apc, COPD_gdppr) AS COPD,
greatest(DEM_op, DEM_apc, DEM_gdppr) AS DEM, 
greatest(DEP_op, DEP_apc, DEP_gdppr) AS DEP,
greatest(DIA_op, DIA_apc, DIA_gdppr) AS DIA,
greatest(DIV_op, DIV_apc, DIV_gdppr) AS DIV,
greatest(EPI_op, EPI_apc, EPI_gdppr) AS EPI,
greatest(HF_op, HF_apc, HF_gdppr) AS HF,
greatest(HL_op, HL_apc, HL_gdppr) AS HL,
greatest(HYP_op, HYP_apc, HYP_gdppr) AS HYP,
greatest(IBD_op, IBD_apc, IBD_gdppr) AS IBD,
greatest(IBS_op, IBS_apc, IBS_gdppr) AS IBS,
greatest(MIG_op, MIG_apc, MIG_gdppr) AS MIG,
greatest(MS_op, MS_apc, MS_gdppr) AS MS,
greatest(PUD_op, PUD_apc, PUD_gdppr) AS PUD,
-- greatest(PNC_op, PNC_apc, PNC_gdppr) AS PNC, -- deactivated because we actually don't have these codes
greatest(PRK_op, PRK_apc, PRK_gdppr) AS PRK,
greatest(PSD_op, PSD_apc, PSD_gdppr) AS PSD,
greatest(PSM_op, PSM_apc, PSM_gdppr) AS PSM,
greatest(PSO_op, PSO_apc, PSO_gdppr) AS PSO,
greatest(PVD_op, PVD_apc, PVD_gdppr) AS PVD,
greatest(RHE_op, RHE_apc, RHE_gdppr) AS RHE,
greatest(SCZ_op, SCZ_apc, SCZ_gdppr) AS SCZ,
greatest(SIN_op, SIN_apc, SIN_gdppr) AS SIN,
greatest(STR_op, STR_apc, STR_gdppr) AS STR,
greatest(THY_op, THY_apc, THY_gdppr) AS THY
FROM .ccu030_${dt}_patient_skinny_record_enhanced18

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced18_pca_id;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced18_pca_id USING PARQUET AS
SELECT 
greatest(ALC_op, ALC_apc, ALC_gdppr) AS ALC,
greatest(AB_op, AB_apc, AB_gdppr) AS AB,
greatest(ANX_op, ANX_apc, ANX_gdppr) AS ANX,
greatest(AST_op, AST_apc, AST_gdppr) AS AST,
greatest(ATR_op, ATR_apc, ATR_gdppr) AS ATR,
greatest(BLI_op, BLI_apc, BLI_gdppr) AS BLI,
greatest(BRO_op, BRO_apc, BRO_gdppr) AS BRO,
greatest(CAN_op, CAN_apc, CAN_gdppr) AS CAN,
greatest(CHD_op, CHD_apc, CHD_gdppr) AS CHD,
greatest(CKD_op, CKD_apc, CKD_gdppr) AS CKD,
greatest(CLD_op, CLD_apc, CLD_gdppr) AS CLD,
-- greatest(CSP_op, CSP_apc, CSP_gdppr) AS CSP, -- deactivated because we actually don't have these codes
greatest(COPD_op, COPD_apc, COPD_gdppr) AS COPD,
greatest(DEM_op, DEM_apc, DEM_gdppr) AS DEM, 
greatest(DEP_op, DEP_apc, DEP_gdppr) AS DEP,
greatest(DIA_op, DIA_apc, DIA_gdppr) AS DIA,
greatest(DIV_op, DIV_apc, DIV_gdppr) AS DIV,
greatest(EPI_op, EPI_apc, EPI_gdppr) AS EPI,
greatest(HF_op, HF_apc, HF_gdppr) AS HF,
greatest(HL_op, HL_apc, HL_gdppr) AS HL,
greatest(HYP_op, HYP_apc, HYP_gdppr) AS HYP,
greatest(IBD_op, IBD_apc, IBD_gdppr) AS IBD,
greatest(IBS_op, IBS_apc, IBS_gdppr) AS IBS,
greatest(MIG_op, MIG_apc, MIG_gdppr) AS MIG,
greatest(MS_op, MS_apc, MS_gdppr) AS MS,
greatest(PUD_op, PUD_apc, PUD_gdppr) AS PUD,
-- greatest(PNC_op, PNC_apc, PNC_gdppr) AS PNC, -- deactivated because we actually don't have these codes
greatest(PRK_op, PRK_apc, PRK_gdppr) AS PRK,
greatest(PSD_op, PSD_apc, PSD_gdppr) AS PSD,
greatest(PSM_op, PSM_apc, PSM_gdppr) AS PSM,
greatest(PSO_op, PSO_apc, PSO_gdppr) AS PSO,
greatest(PVD_op, PVD_apc, PVD_gdppr) AS PVD,
greatest(RHE_op, RHE_apc, RHE_gdppr) AS RHE,
greatest(SCZ_op, SCZ_apc, SCZ_gdppr) AS SCZ,
greatest(SIN_op, SIN_apc, SIN_gdppr) AS SIN,
greatest(STR_op, STR_apc, STR_gdppr) AS STR,
greatest(THY_op, THY_apc, THY_gdppr) AS THY
FROM .ccu030_${dt}_patient_skinny_record_enhanced18
where id = 1

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19_pca;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19_pca USING PARQUET AS
SELECT 
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY
FROM .ccu030_${dt}_patient_skinny_record_enhanced19

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19_pca_id;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced19_pca_id USING PARQUET AS
SELECT 
ALC,AB,ANX,AST,ATR,BLI,BRO,CAN,CHD,CKD,CLD,COPD,DEM,DEP,DIA,DIV,EPI,HF,HL,HYP,IBD,IBS,MIG,MS,PUD,PRK,PSD,PSM,PSO,PVD,RHE,SCZ,SIN,STR,THY
FROM .ccu030_${dt}_patient_skinny_record_enhanced19
where id = 1

-- COMMAND ----------

-- MAGIC %md drop tables that are not needed:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_rows = spark.sql("show tables in  like '*ccu030*'").collect()
-- MAGIC 
-- MAGIC # add keywords for tables that you don't want to drop
-- MAGIC keywords = [f'{dt}', "caliber", "Charlson"]
-- MAGIC for table_row in table_rows:
-- MAGIC   if not any(keyword in table_row.tableName for keyword in keywords):
-- MAGIC #     print(table_row.tableName)
-- MAGIC # activate the line below when ready
-- MAGIC     spark.sql(f"drop table {table_row.database}.{table_row.tableName}")
