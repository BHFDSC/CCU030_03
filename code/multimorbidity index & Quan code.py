# Databricks notebook source
# MAGIC %md **1. data management:**

# Scikit-Learn ?0.20 is required
import sklearn
assert sklearn.__version__ >= "0.20"
print(sklearn.__version__)

# Common imports
import numpy as np
import os
import pandas as pd

# MAGIC %md convert the table to a spark df:

import pyspark.sql.functions as f
df = spark.sql('select * from .ccu030_20221015_1600_patient_skinny_record_enhanced33')

# drop three columns that are not needed
df = df.drop('NHS_NUMBER_DEID', 'covid_admission_date', 'CENSOR_DATE_START', 'covid_date')

# replace nulls with zeros on some columns
df = df.fillna(value=0)

# compress the dataset
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import ByteType

for var in df.columns:
  df = df.withColumn(f'{var}', f.col(f'{var}').cast(ByteType()))

pdf = df.toPandas()

# force all variables to be numeric
pdf = pdf.apply(pd.to_numeric, errors='coerce')

# COMMAND ----------

# MAGIC %md **2. Feature selection:**

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import cross_val_score

pdf2 = pdf[[col for col in pdf.columns if "_apc" in col]]

remaining = set(pdf2.columns)
selected_ini = ['age_at_covid']
selected = []
current_score, best_new_score = 0.000, 0.000

while remaining and current_score >= best_new_score:
  scores_with_candidates = []
  for candidate in remaining:
    c = [candidate]
    temp = [*set(selected + selected_ini)]
    temp.extend(c)
    df_temp = pdf[temp]
    mymodel = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", random_state=42)).fit(df_temp, pdf['covid_death'])
    scores = cross_val_score(mymodel, df_temp, pdf['covid_death'], scoring = "roc_auc", cv=5)
    score = scores.mean()
    scores_with_candidates.append((score, candidate))
  scores_with_candidates.sort()
  best_new_score, best_candidate = scores_with_candidates.pop()
  remaining.remove(best_candidate)
  if best_new_score > (current_score + 0.001):      
    mylist = selected + selected_ini + best_candidate.split()
    df_temp = pdf[mylist]
    mymodel = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", random_state=42)).fit(df_temp, pdf['covid_death'])
    results = pd.DataFrame({"Feature":df_temp.columns.tolist(),"Coefficients":mymodel.steps[1][1].coef_[0]})
    results2 = results[(results['Coefficients']>0)]
    results3 = list(results2["Feature"])
    selected = results3.copy()
    if best_candidate in results3:
      current_score = best_new_score
    else:  
      best_new_score = 0.000      
      
print("selected variables: ", [*set(selected_ini + selected)])
# 'age_at_covid', 'DIA_apc', 'CHD_apc', 'THY_apc'

# COMMAND ----------

# main model

# get AUC
myvars = ['age_at_covid', 'DIA_apc', 'CHD_apc', 'THY_apc']
df_temp = pdf[myvars]
mymodel = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", random_state=42)).fit(df_temp, pdf['covid_death'])
scores = cross_val_score(mymodel, df_temp, pdf['covid_death'], scoring = "roc_auc", cv=5)
print("model vars: \n", myvars, "mean score: \n", scores.mean())

# get p-values from that model
df_temp = pdf[['age_at_covid', 'DIA_apc', 'CHD_apc', 'THY_apc']]
import statsmodels.api as sm
my_model = sm.Logit(pdf['covid_death'], sm.add_constant(df_temp)).fit(disp=0)
my_model.summary()

# get 95% CI for ORs
params = my_model.params
conf = my_model.conf_int()
conf['Odds Ratio'] = params
conf.columns = ['5%', '95%', 'Odds Ratio']
print(np.exp(conf))

# COMMAND ----------

# model with bespoke multimorbidity index

pdf["MMI_apc"] = 1.2088 * pdf['DIA_apc'] + 0.6712 * pdf["THY_apc"] + 0.7061 * pdf["CHD_apc"]

# get AUC
myvars = ['age_at_covid', 'MMI_apc']
df_temp = pdf[myvars]
mymodel = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", random_state=42)).fit(df_temp, pdf['covid_death'])
scores = cross_val_score(mymodel, df_temp, pdf['covid_death'], scoring = "roc_auc", cv=5)
print("model vars: \n", myvars, "mean score: \n", scores.mean())  

# Compute 95% confidence interval for AUC using t-distribution
from scipy.stats import sem, t
mean_auc = np.mean(scores)
sem_auc = sem(scores)  # Standard error of the mean
confidence = 0.95
dfs = len(scores) - 1  # Degrees of freedom
t_value = t.ppf((1 + confidence) / 2, dfs)  # Two-tailed t-value
margin_of_error = t_value * sem_auc
lower_bound = mean_auc - margin_of_error
upper_bound = mean_auc + margin_of_error
print(f"Mean AUC: {mean_auc:.4f}")
print(f"95% Confidence Interval: ({lower_bound:.4f}, {upper_bound:.4f})")

# get p-values from that model
df_temp = pdf[['age_at_covid', 'MMI_apc']]
import statsmodels.api as sm
my_model = sm.Logit(pdf['covid_death'], sm.add_constant(df_temp)).fit(disp=0)
my_model.summary()

# get 95% CI for ORs
params = my_model.params
conf = my_model.conf_int()
conf['Odds Ratio'] = params
conf.columns = ['5%', '95%', 'Odds Ratio']
print(np.exp(conf))

# COMMAND ----------

# model with Quan Index

# get AUC
myvars = ['age_at_covid', 'quan_index2']
df_temp = pdf[myvars]
mymodel = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", random_state=42)).fit(df_temp, pdf['covid_death'])
scores = cross_val_score(mymodel, df_temp, pdf['covid_death'], scoring = "roc_auc", cv=5)
print("model vars: \n", myvars, "mean score: \n", scores.mean())

# Compute 95% confidence interval for AUC using t-distribution
from scipy.stats import sem, t
mean_auc = np.mean(scores)
sem_auc = sem(scores)  # Standard error of the mean
confidence = 0.95
dfs = len(scores) - 1  # Degrees of freedom
t_value = t.ppf((1 + confidence) / 2, dfs)  # Two-tailed t-value
margin_of_error = t_value * sem_auc
lower_bound = mean_auc - margin_of_error
upper_bound = mean_auc + margin_of_error
print(f"Mean AUC: {mean_auc:.4f}")
print(f"95% Confidence Interval: ({lower_bound:.4f}, {upper_bound:.4f})")

# get p-values from that model
df_temp = pdf[['age_at_covid', 'quan_index2']]
import statsmodels.api as sm
my_model = sm.Logit(pdf['covid_death'], sm.add_constant(df_temp)).fit(disp=0)
my_model.summary()

# get 95% CI for ORs
params = my_model.params
conf = my_model.conf_int()
conf['Odds Ratio'] = params
conf.columns = ['5%', '95%', 'Odds Ratio']
print(np.exp(conf))
