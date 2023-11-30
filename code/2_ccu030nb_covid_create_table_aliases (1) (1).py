# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **This notebook creates input tables for the main datasets required for the COVID phenotyping work.**

# COMMAND ----------

# MAGIC %python
# MAGIC dt = dbutils.widgets.get("dt")

# COMMAND ----------

dbutils.notebook.run("ccu030nb_helper_functions", 3600)

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring, regexp_replace, max
from pyspark.sql import functions as f
from datetime import datetime, date
from pyspark.sql.types import DateType

# COMMAND ----------

start_date = date(2020, 1, 1)
print(f"Start date is {start_date}") 

# COMMAND ----------

end_date = date(2021, 12, 31)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Subseting all source tables by dates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 SGSS

# COMMAND ----------

# SGSS
sgss = spark.sql(f'''
                    SELECT person_id_deid, REPORTING_LAB_ID, specimen_date 
                    FROM .ccu030_{dt}_sgss
                    ''')
sgss = sgss.withColumnRenamed('specimen_date', 'date')
sgss = sgss.withColumn('date_is', lit('specimen_date'))
sgss = sgss.filter((sgss['date'] >= start_date) & (sgss['date'] <= end_date))
sgss = sgss.filter(sgss['person_id_deid'].isNotNull())
# sgss.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_sgss')
sgss.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_sgss")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_sgss OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM .ccu030_${dt}_tmp_sgss

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 GDPPR

# COMMAND ----------

# GDPPR
gdppr = spark.sql(f'''
                    SELECT distinct NHS_NUMBER_DEID, DATE, CODE, ProductionDate 
                    FROM .ccu030_{dt}_gdppr 
                     ''')
gdppr = gdppr.withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid')
gdppr = gdppr.withColumn('date_is', lit('DATE'))
gdppr = gdppr.filter((gdppr['date'] >= start_date) & (gdppr['date'] <= end_date))
gdppr = gdppr.filter(gdppr['person_id_deid'].isNotNull())
#gdppr.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_gdppr')
#display(gdppr)
gdppr.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_gdppr")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_gdppr OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM .ccu030_${dt}_tmp_gdppr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Deaths

# COMMAND ----------

# Deaths
death = spark.sql(f'''
                    SELECT * 
                    FROM .ccu030_{dt}_deaths
                  ''')
death = death.withColumn("death_date", to_date(death['REG_DATE_OF_DEATH'], "yyyyMMdd"))
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id_deid')
death = death.withColumn('date_is', lit('REG_DATE_OF_DEATH'))
death = death.filter((death['death_date'] >= start_date) & (death['death_date'] <= end_date))
death = death.filter(death['person_id_deid'].isNotNull())
#death.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_deaths')
death.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_deaths")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_deaths OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(death_date), max(death_date) FROM .ccu030_${dt}_tmp_deaths

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 HES APC

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
apc = spark.sql(f'''
                    SELECT PERSON_ID_DEID, EPISTART, DIAG_4_CONCAT, OPERTN_4_CONCAT, DISMETH, DISDEST, DISDATE, SUSRECID 
                    FROM .ccu030_{dt}_hes_apc_all_years
                    WHERE DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%"
                    ''')
apc = apc.withColumnRenamed('EPISTART', 'date')
apc = apc.withColumn('date_is', lit('EPISTART'))
apc = apc.filter((apc['date'] >= start_date) & (apc['date'] <= end_date))
apc = apc.filter(apc['person_id_deid'].isNotNull())
#apc.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_apc')
#display(apc)
apc.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_apc")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_apc OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM .ccu030_${dt}_tmp_apc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 HES CC

# COMMAND ----------

# HES CC
cc = spark.sql(f'''
              SELECT * 
              FROM .ccu030_{dt}_hes_cc_all_years
              ''')
cc = cc.withColumnRenamed('CCSTARTDATE', 'date')
cc = cc.withColumn('date_is', lit('CCSTARTDATE'))
# reformat dates for hes_cc as currently strings
asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
cc = cc.filter(cc['person_id_deid'].isNotNull())
cc = cc.withColumn('date', asDate(col('date')))
cc = cc.filter((cc['date'] >= start_date) & (cc['date'] <= end_date))
#cc.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_cc3')
cc.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_cc3")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_cc3 OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM .ccu030_${dt}_tmp_cc3

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Pillar 2 Antigen testing **Not currently in use**
# MAGIC Briefly this dataset should be entirely encapsulated within SGSS (which includes Pilars 1 & 2), however this was not the case. We were additionally detecting multiple tests per indidivdual, in contrast to the dataset specification. This dataset has currently been recalled by NHS-Digital.
# MAGIC 
# MAGIC FS update: this looks correct - Pillar 2 should be within SGSS. Drop.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 CHESS
# MAGIC * Previously we weren't using the `_archive` table as it wasn't updated/didn't exist

# COMMAND ----------

# chess = spark.sql('''SELECT PERSON_ID_DEID as person_id_deid, Typeofspecimen, Covid19, AdmittedToICU, Highflownasaloxygen, NoninvasiveMechanicalventilation, Invasivemechanicalventilation,
#                       RespiratorySupportECMO, DateAdmittedICU, HospitalAdmissionDate, InfectionSwabDate as date, 'InfectionSwabDate' as date_is 
#                       FROM .chess_''')    
chess = spark.sql(f'''
                     SELECT PERSON_ID_DEID as person_id_deid, Typeofspecimen, Covid19, AdmittedToICU, Highflownasaloxygen, NoninvasiveMechanicalventilation, Invasivemechanicalventilation,
                     RespiratorySupportECMO, DateAdmittedICU, HospitalAdmissionDate, InfectionSwabDate as date, 'InfectionSwabDate' as date_is 
                     FROM .ccu030_{dt}_chess 
                     ''')
chess = chess.filter(chess['Covid19'] == 'Yes')
chess = chess.filter(chess['person_id_deid'].isNotNull())
#chess = chess.filter((chess['date'] >= start_date) & (chess['date'] <= end_date))
chess = chess.filter(((chess['date'] >= start_date) | (chess['date'].isNull())) & ((chess['date'] <= end_date) | (chess['date'].isNull())))
chess = chess.filter(((chess['HospitalAdmissionDate'] >= start_date) | (chess['HospitalAdmissionDate'].isNull())) & ((chess['HospitalAdmissionDate'] <= end_date) |(chess['HospitalAdmissionDate'].isNull())))
chess = chess.filter(((chess['DateAdmittedICU'] >= start_date) | (chess['DateAdmittedICU'].isNull())) & ((chess['DateAdmittedICU'] <= end_date) | (chess['DateAdmittedICU'].isNull())))
#chess.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_chess')
chess.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_chess")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_chess OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(date), max(date) FROM .ccu030_${dt}_tmp_chess

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 SUS
# MAGIC NB this is a large dataset and the coalescing of diagnosis & procedure fields into a format compatible with our HES queries takes some time (~40 mins)

# COMMAND ----------

## SUS
# ! Takes ~ 40 mins 
sus = spark.sql('''
SELECT NHS_NUMBER_DEID, EPISODE_START_DATE, 
CONCAT (COALESCE(PRIMARY_DIAGNOSIS_CODE, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_1, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_2, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_3, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_4, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_5, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_6, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_7, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_8, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_9, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_10, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_11, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_12, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_13, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_14, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_15, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_16, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_17, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_18, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_19, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_20, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_21, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_22, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_23, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_24, '')) as DIAG_CONCAT,
CONCAT (COALESCE(PRIMARY_PROCEDURE_CODE, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_2, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''),  ',',
COALESCE(SECONDARY_PROCEDURE_CODE_3, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_4, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_5, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_6, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_7, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_8, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_9, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_10, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_11, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_12, '')) as PROCEDURE_CONCAT, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_DATE_1,
DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL, DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL 
FROM .sus_
''')
## Other potential interstersting columns: END_DATE_HOSPITAL_PROVIDER_SPELL, EPISODE_START_DATE, EPISODE_END_DATE, PRIMARY_PROCEDURE_CODE, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_CODE1 - 12
sus = sus.withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('EPISODE_START_DATE', 'date')
sus = sus.withColumn('date_is', lit('EPISODE_START_DATE'))
sus = sus.filter((sus['date'] >= start_date) & (sus['date'] <= end_date))
sus = sus.filter(((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] >= start_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())) & ((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] <= end_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())))
sus = sus.filter(sus['person_id_deid'].isNotNull()) # Loads of rows with missing IDs
sus = sus.filter(sus['date'].isNotNull())
#sus.createOrReplaceGlobalTempView('ccu030_{dt}_tmp_sus')
sus.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_tmp_sus")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .ccu030_${dt}_tmp_sus OWNER TO 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(END_DATE_HOSPITAL_PROVIDER_SPELL) FROM .ccu030_${dt}_tmp_sus

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(date) FROM .ccu030_${dt}_tmp_sus

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Hardcode SNOMED-CT codes for COVID-19

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED COVID-19 codes
# MAGIC -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Covid-1 Status groups:
# MAGIC -- - Lab confirmed incidence
# MAGIC -- - Lab confirmed historic
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu030_${dt}_snomed_codes_covid19 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# MAGIC ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# MAGIC ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# MAGIC ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# MAGIC ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# MAGIC ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# MAGIC ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# MAGIC ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# MAGIC ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# MAGIC ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
# MAGIC ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# MAGIC ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# MAGIC ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
# MAGIC 
# MAGIC AS tab(clinical_code, description, sensitive_status, include_binary, covid_phenotype);

# COMMAND ----------


