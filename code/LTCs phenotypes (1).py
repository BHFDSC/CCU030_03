# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Prepare Cambridge CPRD tables for long term condition concepts

# COMMAND ----------

import databricks.koalas as ks
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

# get all phenotyping tables for "CPRD@Cambridge Codelists"
phen_tables = ks.sql(f"SHOW TABLES FROM bhf_cvd_covid_uk_byod").to_pandas()
print(phen_tables.columns)
phen_tables = phen_tables.loc[phen_tables['tableName'].str.contains("cam_cprd") & ~phen_tables['tableName'].str.contains("snomed")]  # ICD and not Snomed 
# pd.set_option("display.max_rows", None, "display.max_columns", None)
phen_tables = phen_tables['tableName'].to_numpy().tolist()
print(len(phen_tables))  # -> 47 tables; website (https://www.phpc.cam.ac.uk/pcu/research/research-groups/crmh/cprd_cam/codelists/v11/) states there should be 46 -> one delta is 'cam_cprdcam_hypercholesterolaemia'
# remove 'cam_cprdcam_hypercholesterolaemia' in list
phen_tables.remove('cam_cprdcam_hypercholesterolaemia')
print(len(phen_tables)) # now 46 -> correct
print(phen_tables)

# COMMAND ----------

# value is [condition description, condition short (see Angela 1 paper, e.g. just 'Anxiety'), condition abbreviation (e.g. 'HYP')]
# Note: condition short were amended, wheverever better; condition abbreviation remains unchanged
list_name_to_condition = {
  'alc138': ['Alcohol problems', 'Alcohol problems', 'ALC'], 
  'ano139': ['Anorexia or bulimia', 'Anorexia or bulimia', 'AB'], 
  'anx140': ['Anxiety & other neurotic, stress related & somatoform disorders', 'Anxiety', 'ANX'], 
  'anx141': ['Anxiety & other neurotic, stress related & somatoform disorders', 'Anxiety', 'ANX'], 
  'ast127': ['Asthma (currently treated)', 'Asthma', 'AST'], 
  'ast142': ['Asthma (currently treated)', 'Asthma', 'AST'], 
  'atr143': ['Atrial fibrillation', 'Atrial fibrillation', 'ATR'], 
  'bli144': ['Blindness and low vision', 'Blindness and low vision', 'BLI'], 
  'bro145': ['Bronchiectasis', 'Bronchiectasis', 'BRO'], 
  'can146': ['Cancer - [New] Diagnosis in last five years', 'Cancer', 'CAN'], 
  'chd126': ['Coronary heart disease', 'Coronary heart disease', 'CHD'], 
  'ckd147': ['Chronic kidney disease', 'Chronic kidney disease', 'CKD'], 
  'cld148': ['Chronic Liver Disease and Viral Hepatitis', 'Chronic Liver Disease', 'CLD'], 
  'con150': ['Constipation (Treated)', 'Constipation', 'CSP'], 
  'cop151': ['COPD', 'COPD', 'COPD'], 
  'dem131': ['Dementia', 'Dementia', 'DEM'],  
  'dep152': ['Depression', 'Depression', 'DEP'], 
  'dep153': ['Depression', 'Depression', 'DEP'], 
  'dib128': ['Diabetes', 'Diabetes', 'DIA'], 
  'div154': ['Diverticular disease of intestine', 'Diverticular disease of intestine', 'DIV'], 
  'epi155': ['Epilepsy (currently treated)', 'Epilepsy', 'EPI'], 
  'epi156': ['Epilepsy (currently treated)', 'Epilepsy', 'EPI'], 
  'hef158': ['Heart failure', 'Heart failure', 'HF'], 
  'hel157': ['Hearing loss', 'Hearing loss', 'HL'], 
  'hyp159': ['Hypertension', 'Hypertension', 'HYP'], 
  'ibd160': ['Inflammatory bowel disease', 'Inflammatory bowel disease', 'IBD'], 
  'ibs161': ['Irritable bowel syndrome', 'Irritable bowel syndrome', 'IBS'], 
  'ibs162': ['Irritable bowel syndrome', 'Irritable bowel syndrome', 'IBS'], 
  'lea163': ['Learning disability', 'Learning disability', 'LD'], 
  'mig164': ['Migraine', 'Migraine', 'MIG'], 
  'msc165': ['Multiple sclerosis', 'Multiple sclerosis', 'MS'], 
  'pep135': ['Peptic Ulcer Disease', 'Peptic Ulcer Disease', 'PUD'], 
  'pnc166': ['Painful condition', 'Painful condition', 'PNC'], 
  'pnc167': ['Painful condition', 'Painful condition', 'PNC'], 
  'prk169': ["Parkinson's disease", "Parkinson's disease", 'PRK'], 
  'pro170': ['Prostate disorders', 'Prostate disorders', 'PSD'], 
  'psm173': ['Psychoactive substance misuse (NOT ALCOHOL)', 'Psychoactive substance misuse (not alcohol)', 'PSM'], 
  'pso171': ['Psoriasis or eczema', 'Psoriasis or eczema', 'PSO'], 
  'pso172': ['Psoriasis or eczema', 'Psoriasis or eczema', 'PSO'], 
  'pvd168': ['Peripheral vascular disease', 'Peripheral vascular disease', 'PVD'], 
  'rhe174': ['Rheumatoid arthritis, other inflammatory polyarthropathies & systematic connective tissue disorders', 'Rheumatoid arthritis', 'RHE'], 
  'scz175': ['Schizophrenia (and related non-organic psychosis) or bipolar disorder', 'Schizophrenia', 'SCZ'], 
  'scz176': ['Schizophrenia (and related non-organic psychosis) or bipolar disorder', 'Schizophrenia', 'SCZ'], 
  'sin149': ['Chronic sinusitis', 'Chronic sinusitis', 'SIN'], 
  'str130': ['Stroke & transient ischaemic attack', 'Stroke & transient ischaemic attack', 'STR'], 
  'thy179': ['Thyroid disorders', 'Thyroid disorders', 'THY'],                  
}

list_name_to_type = {
  'alc138': 'medcode', 
  'ano139': 'medcode', 
  'anx140': 'medcode', 
  'anx141': 'prodcode', 
  'ast127': 'prodcode', 
  'ast142': 'medcode', 
  'atr143': 'medcode', 
  'bli144': 'medcode', 
  'bro145': 'medcode', 
  'can146': 'medcode', 
  'chd126': 'medcode', 
  'ckd147': 'medcode',  
  'cld148': 'medcode', 
  'con150': 'prodcode', 
  'cop151': 'medcode', 
  'dem131': 'medcode', 
  'dep152': 'medcode', 
  'dep153': 'prodcode', 
  'dib128': 'medcode', 
  'div154': 'medcode', 
  'epi155': 'medcode', 
  'epi156': 'prodcode', 
  'hef158': 'medcode', 
  'hel157': 'medcode', 
  'hyp159': 'medcode', 
  'ibd160': 'medcode', 
  'ibs161': 'medcode', 
  'ibs162': 'prodcode', 
  'lea163': 'medcode', 
  'mig164': 'prodcode', 
  'msc165': 'medcode', 
  'pep135': 'medcode', 
  'pnc166': 'prodcode', 
  'pnc167': 'prodcode', 
  'prk169': 'medcode', 
  'pro170': 'medcode', 
  'psm173': 'medcode', 
  'pso171': 'medcode',  
  'pso172': 'prodcode', 
  'pvd168': 'medcode', 
  'rhe174': 'medcode', 
  'scz175': 'medcode', 
  'scz176': 'prodcode', 
  'sin149': 'medcode', 
  'str130': 'medcode',  
  'thy179': 'medcode'                  
}


df_list = []
# task: add List and Condition info to Cam tables 
for phen_table in phen_tables: 
  print(phen_table)
  # get the table
  df = spark.table(f"bhf_cvd_covid_uk_byod.{phen_table}").toPandas()
  list_name = phen_table.split("_")[2]
  if list_name_to_type[list_name] == 'medcode': 
    # get a subset of the relevant columns
    df = df[['medcode', 'readcode', 'Description']]
    # rename columns
    df = df.rename(columns={'medcode': 'Medcode', 'readcode': 'Readcode_7char', 'Description': 'Medcode_description'})
    # add List info to table
    df['List'] = list_name  # e.g. 'alc138'
    # add condition info to table
    df['Condition_description'] = list_name_to_condition[list_name][1]
    df['Condition_abbreviation'] = list_name_to_condition[list_name][2]
    # create 5 char readcode
    df['Readcode_5char'] = df['Readcode_7char'].str[:5]  # assumption: first 5 characters of 7 char readcode is 5 char readcode
    # reorder table columns
    df = df[['List', 'Condition_abbreviation', 'Condition_description', 'Readcode_7char', 'Readcode_5char', 'Medcode', 'Medcode_description']]
    # append to list
    df_list.append(df)
  elif list_name_to_type[list_name] == 'prodcode': 
    pass  # TODO

print(len(df_list))
print(df_list[0])

df_merged = pd.concat(df_list)

print("printing df_merged...")
print(df_merged)
print("saving df_merged...")
# df_merged.to_csv("dars.nic.391419_j3w9t_collab.test", index=False)


# TODO deal with prodcodes
# TODO save the one merged table

# COMMAND ----------

## Map readcodes to ICD10 

# Read code (5 char) to ICD-10 codes
df_map = spark.table('dss_corporate.read_codes_ctv3_to_icd10').toPandas()
key_read_code = df_map['CTV3_READ_CODE'].to_numpy().tolist()
value_icd10 = df_map['TARGET_CODE'].to_numpy().tolist()
# dictionary for the mapping, created from the key and value list
read_code_to_icd10 = dict(zip(key_read_code, value_icd10))
icd10_to_read_code = dict(zip(value_icd10, key_read_code))
# apply dictionary to series
icd10_series = df_merged['Readcode_5char'].map(read_code_to_icd10)
# put series in dictionary
df_merged['ICD10'] = icd10_series

print(df_merged.head())
print(df_merged)
len(df_merged['List'].unique().tolist())

# COMMAND ----------

# fraction of non-NA values
counts = df_merged.count()
print(counts['ICD10'] / df_merged.shape[0])

# COMMAND ----------

df_merged[df_merged.ICD10.isna()]

# COMMAND ----------

df_map

# COMMAND ----------

# read_codes_ctv3_to_icd10 -> for mapping of readcodes to ICD10

phen_table = phen_tables[0]
df = spark.table(f"bhf_cvd_covid_uk_byod.{phen_table}").toPandas()

print("here")

# ks.sql(f"SHOW COLUMNS FROM .{selected_table}").to_pandas()['col_name']
phen = spark.table('bhf_cvd_covid_uk_byod.cam_cprdcam_scz175_mc_v1_1_oct2018')
display(phen)

# phen = spark.table('bhf_cvd_covid_uk_byod.caliber_cprd_benign_uterus')
# display(phen)

# COMMAND ----------

mapping = spark.table('dss_corporate.read_codes_ctv3_to_icd10')
display(mapping)
