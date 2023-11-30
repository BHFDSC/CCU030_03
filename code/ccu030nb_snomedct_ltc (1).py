# Databricks notebook source
# MAGIC %python
# MAGIC dt = dbutils.widgets.get("dt")
# MAGIC print(dt)

# COMMAND ----------

# MAGIC %md ## First get list of tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM bhf_cvd_covid_uk_byod like 'cam_cprdcam*_mc_*_snomedct'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM bhf_cvd_covid_uk_byod like '*_snomedct' 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct phenotype
# MAGIC -- from .ccu013_caliber_master_codelist
# MAGIC -- where terminology like '%SNOMEDCT%' and phenotype like '%thyro%'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select *
# MAGIC -- from .ccu013_caliber_master_codelist

# COMMAND ----------

database_name = 'bhf_cvd_covid_uk_byod'
df_tables_cam_cprd_cam_mc = spark.sql(f"SHOW TABLES FROM {database_name} like 'cam_cprdcam*_mc_*_snomedct'").toPandas()

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col

list_of_cam_cprd_cam_mc_tables = [spark.table(f"{database_name}.{table_name}").withColumn('code_list',lit(table_name)) for table_name in df_tables_cam_cprd_cam_mc.tableName.to_list()]
df_combined = reduce(DataFrame.union, list_of_cam_cprd_cam_mc_tables)
df_combined.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_cam_cprdcam_mc_snomedct4")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp4 using parquet as
# MAGIC select *, substring(code_list, 13, 6) AS cond_code
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct4

# COMMAND ----------

# MAGIC %md repeating this operation for three tables saved in  rather than BYOD

# COMMAND ----------

database_name = ''
df_tables_cam_cprd_cam_mc = spark.sql(f"SHOW TABLES FROM {database_name} like 'cam_cprdcam*_mc_*_snomedct'").toPandas()

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col

list_of_cam_cprd_cam_mc_tables = [spark.table(f"{database_name}.{table_name}").withColumn('code_list',lit(table_name)) for table_name in df_tables_cam_cprd_cam_mc.tableName.to_list()]
df_combined = reduce(DataFrame.union, list_of_cam_cprd_cam_mc_tables)
df_combined.write.format("parquet").mode("overwrite").saveAsTable(f".ccu030_{dt}_cam_cprdcam_mc_snomedct5")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp5 using parquet as
# MAGIC select *, substring(code_list, 13, 6) AS cond_code
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct5

# COMMAND ----------

# MAGIC %md appending the two tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp6 using parquet as
# MAGIC select *
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp4
# MAGIC UNION ALL
# MAGIC select *
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp5

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp7 using parquet as
# MAGIC select S.*, H.cond_description, H.cond_short, H.cond_abbreviation, I.icd10 
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp6 S
# MAGIC left join .ccu030_${dt}_ltc_helper_table H ON S.cond_code = H.cond_code
# MAGIC left join .ccu030_${dt}_snomed_icd10_lookup I ON S.conceptId = I.snomed

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists .ccu030_${dt}_cam_cprdcam_mc_snomedct_final using parquet as
# MAGIC select distinct *
# MAGIC from .ccu030_${dt}_cam_cprdcam_mc_snomedct_temp7
