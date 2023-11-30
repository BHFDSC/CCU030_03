# Databricks notebook source
# MAGIC %md # ccu030nb Master Notebook

# COMMAND ----------

# MAGIC %md **This notebook runs data management sub-notebooks in the right sequence**

# COMMAND ----------

my_production_date = "2022-06-29 00:00:00.000000"
my_pc_meds_production_date = "2022-04-25 00:00:00.000000"

# COMMAND ----------

from datetime import datetime
import random

dt = datetime.now().strftime("%Y%m%d_%H%M")
print(dt)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.notebook.run("1_ccu030nb_create_tables", 30000, {"dt": f"{dt}", "my_production_date": f"{my_production_date}", "my_pc_meds_production_date": f"{my_pc_meds_production_date}"})

# COMMAND ----------

dbutils.notebook.run("2_ccu030nb_covid_create_table_aliases", 30000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("3_ccu030nb_covid_treatment", 30000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("4_ccu030nb_covid_master_phenotypes", 30000, {"dt": f"{dt}"})
# keep only confirmed records!
# creates proper tables 'ccu030nb_covid_trajectory' and 'ccu030nb_covid_severity'

# COMMAND ----------

dbutils.notebook.run("5_ccu030nb_covid_jabs", 30000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("6_ccu030nb_gdppr", 150000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("7_ccu030nb_hes_ltc", 60000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("8_ccu030nb_polypharmacy", 60000, {"dt": f"{dt}"})

# COMMAND ----------

dbutils.notebook.run("9_ccu030nb_merger", 250000, {"dt": f"{dt}"})
# giving it three days to run

# COMMAND ----------

dbutils.notebook.run("10_ccu030nb_mopup", 60000, {"dt": f"{dt}"})
