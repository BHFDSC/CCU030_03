-- Databricks notebook source
-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")

-- COMMAND ----------

-- MAGIC %md first merge the skinny with covid status, ID and autism:

-- COMMAND ----------

drop table if exists .ccu030_${dt}_patient_skinny_record_enhanced16;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced16 USING DELTA AS
SELECT t.*, 
t2.covid_severity, t2.covid_status, t2.covid_date, 
t14.n_doses, t14.VACCINE_PRODUCT_CODE, 
t15.on_id_register, t15.id, t15.id_severity,
t22.autism
FROM .ccu030_${dt}_gdppr_skinny t -- all records in my skinny are unique
  LEFT JOIN .ccu030_${dt}_covid_severity t2 ON t.NHS_NUMBER_DEID = t2.person_id_deid 
  LEFT JOIN .ccu030_${dt}_vaccine_status2 t14 ON t.NHS_NUMBER_DEID = t14.person_id_deid
  LEFT JOIN .ccu030_${dt}_id t15 ON t.NHS_NUMBER_DEID = t15.person_id_deid
  LEFT JOIN .ccu030_${dt}_autism t22 ON t.NHS_NUMBER_DEID = t22.person_id_deid  
-- the resulting table should have unique records

-- COMMAND ----------

OPTIMIZE .ccu030_${dt}_patient_skinny_record_enhanced16 ZORDER BY NHS_NUMBER_DEID

-- COMMAND ----------

-- MAGIC %md add autism_no_id indicator:

-- COMMAND ----------

drop table if exists .ccu030_${dt}_patient_skinny_record_enhanced17;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced17 USING DELTA AS
SELECT *, case when autism == 1 and (id != 1 or id is null) then 1 else null end as autism_no_id
FROM .ccu030_${dt}_patient_skinny_record_enhanced16

-- COMMAND ----------

-- MAGIC %md then complete the merger:

-- COMMAND ----------

drop table if exists .ccu030_${dt}_patient_skinny_record_enhanced18;
CREATE TABLE IF NOT EXISTS .ccu030_${dt}_patient_skinny_record_enhanced18 USING DELTA AS
SELECT t.*, 
t3.niv_chess,
t4.niv_cc,
t5.niv_apc,
t6.niv_sus,
t7.imv_chess,
t8.imv_cc,
t9.imv_apc,
t10.imv_sus,
t11.ecmo_chess,
t12.ecmo_apc,
t13.ecmo_sus,
t16.*,
t17.*,
t18.*,
t19.medcount,
t20.*,
t21.ETHNICITY_DESCRIPTION, t21.ETHNIC_GROUP,
t23.time,
t24.vaccine_name,
t25.covid_admission_date, t25.covid_admission,
t26.non_covid_death, t26.non_covid_death_date,
t27.test_date, t27.tested,
t28.antipsychotic, t28.antidepressant, t28.anticonvulsant
FROM .ccu030_${dt}_patient_skinny_record_enhanced17 t 
  LEFT JOIN .ccu030_${dt}_covid_niv_chess2 t3 ON t.NHS_NUMBER_DEID = t3.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_niv_cc2 t4 ON t.NHS_NUMBER_DEID = t4.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_niv_apc2 t5 ON t.NHS_NUMBER_DEID = t5.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_niv_sus2 t6 ON t.NHS_NUMBER_DEID = t6.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_imv_chess2 t7 ON t.NHS_NUMBER_DEID = t7.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_imv_cc2 t8 ON t.NHS_NUMBER_DEID = t8.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_imv_apc2 t9 ON t.NHS_NUMBER_DEID = t9.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_imv_sus2 t10 ON t.NHS_NUMBER_DEID = t10.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_ecmo_chess2 t11 ON t.NHS_NUMBER_DEID = t11.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_ecmo_apc2 t12 ON t.NHS_NUMBER_DEID = t12.person_id_deid
  LEFT JOIN .ccu030_${dt}_covid_ecmo_sus2 t13 ON t.NHS_NUMBER_DEID = t13.person_id_deid
  LEFT JOIN .ccu030_${dt}_gdppr_ltc_final9 t16 ON t.NHS_NUMBER_DEID = t16.person_id_deid_gdppr -- Note the _gdppr at the end.
  LEFT JOIN .ccu030_${dt}_hes_apc_ltc_final t17 ON t.NHS_NUMBER_DEID = t17.person_id_deid_apc -- Note the _apc at the end.
  LEFT JOIN .ccu030_${dt}_hes_op_ltc_final t18 ON t.NHS_NUMBER_DEID = t18.person_id_deid_op -- Note the _op at the end.
  LEFT JOIN .ccu030_${dt}_primary_care_meds3 t19 ON t.NHS_NUMBER_DEID = t19.person_id_deid 
  LEFT JOIN .ccu030_${dt}_gdppr_lsoa_imd t20 ON t.NHS_NUMBER_DEID = t20.person_id_deid 
  LEFT JOIN .ccu030_${dt}_ethnicity_lookup t21 ON t.ETHNIC = t21.ETHNICITY_CODE
  LEFT JOIN .ccu030_${dt}_covid_diagnosis_date6 t23 ON t.NHS_NUMBER_DEID = t23.person_id_deid 
  LEFT JOIN .ccu030_${dt}_vaccine_names t24 ON t.VACCINE_PRODUCT_CODE = t24.VACCINE_PRODUCT_CODE 
  LEFT JOIN .ccu030_${dt}_covid_admission t25 ON t.NHS_NUMBER_DEID = t25.person_id_deid   
  left join .ccu030_${dt}_non_covid_death t26 ON t.NHS_NUMBER_DEID = t26.person_id_deid 
  left join .ccu030_${dt}_covid_tested t27 ON t.NHS_NUMBER_DEID = t27.person_id_deid
  left join .ccu030_${dt}_primary_care_meds4 t28 ON t.NHS_NUMBER_DEID = t28.person_id_deid

-- COMMAND ----------

OPTIMIZE .ccu030_${dt}_patient_skinny_record_enhanced18 ZORDER BY NHS_NUMBER_DEID

-- COMMAND ----------

select *
from .ccu030_${dt}_patient_skinny_record_enhanced18

-- COMMAND ----------


