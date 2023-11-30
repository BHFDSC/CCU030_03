-- Databricks notebook source
-- MAGIC %md **this notebook creates a helper table with LTC codes and names etc.**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")
-- MAGIC print(dt)

-- COMMAND ----------

DROP TABLE IF EXISTS .ccu030_${dt}_ltc_helper_table;

CREATE TABLE .ccu030_${dt}_ltc_helper_table (cond_code STRING, cond_description STRING, cond_short STRING, cond_abbreviation STRING) STORED AS ORC;
INSERT INTO .ccu030_${dt}_ltc_helper_table VALUES 
('alc138', 'Alcohol problems', 'Alcohol problems', 'ALC'),
('ano139', 'Anorexia or bulimia', 'Anorexia or bulimia', 'AB'),
('anx140', 'Anxiety & other neurotic, stress related & somatoform disorders', 'Anxiety', 'ANX'),
('anx141', 'Anxiety & other neurotic, stress related & somatoform disorders', 'Anxiety', 'ANX'),
('ast127', 'Asthma (currently treated)', 'Asthma', 'AST'),
('ast142', 'Asthma (currently treated)', 'Asthma', 'AST'),
('atr143', 'Atrial fibrillation', 'Atrial fibrillation', 'ATR'),
('bli144', 'Blindness and low vision', 'Blindness and low vision', 'BLI'),
('bro145', 'Bronchiectasis', 'Bronchiectasis', 'BRO'),
('can146', 'Cancer - [New] Diagnosis in last five years', 'Cancer', 'CAN'),
('chd126', 'Coronary heart disease', 'Coronary heart disease', 'CHD'),
('ckd147', 'Chronic kidney disease', 'Chronic kidney disease', 'CKD'),
('cld148', 'Chronic Liver Disease and Viral Hepatitis', 'Chronic Liver Disease', 'CLD'),
('con150', 'Constipation (Treated)', 'Constipation', 'CSP'),
('cop151', 'COPD', 'COPD', 'COPD'),
('dem131', 'Dementia', 'Dementia', 'DEM'), 
('dep152', 'Depression', 'Depression', 'DEP'),
('dep153', 'Depression', 'Depression', 'DEP'),
('dib128', 'Diabetes', 'Diabetes', 'DIA'),
('div154', 'Diverticular disease of intestine', 'Diverticular disease of intestine', 'DIV'),
('epi155', 'Epilepsy (currently treated)', 'Epilepsy', 'EPI'),
('epi156', 'Epilepsy (currently treated)', 'Epilepsy', 'EPI'),
('hef158', 'Heart failure', 'Heart failure', 'HF'),
('hel157', 'Hearing loss', 'Hearing loss', 'HL'),
('hyp159', 'Hypertension', 'Hypertension', 'HYP'),
('ibd160', 'Inflammatory bowel disease', 'Inflammatory bowel disease', 'IBD'),
('ibs161', 'Irritable bowel syndrome', 'Irritable bowel syndrome', 'IBS'),
('ibs162', 'Irritable bowel syndrome', 'Irritable bowel syndrome', 'IBS'),
('lea163', 'Learning disability', 'Learning disability', 'LD'),
('mig164', 'Migraine', 'Migraine', 'MIG'),
('msc165', 'Multiple sclerosis', 'Multiple sclerosis', 'MS'),
('pep135', 'Peptic Ulcer Disease', 'Peptic Ulcer Disease', 'PUD'),
('pnc166', 'Painful condition', 'Painful condition', 'PNC'),
('pnc167', 'Painful condition', 'Painful condition', 'PNC'),
('prk169', "Parkinson's disease", "Parkinson's disease", 'PRK'),
('pro170', 'Prostate disorders', 'Prostate disorders', 'PSD'),
('psm173', 'Psychoactive substance misuse (NOT ALCOHOL)', 'Psychoactive substance misuse (not alcohol)', 'PSM'),
('pso171', 'Psoriasis or eczema', 'Psoriasis or eczema', 'PSO'),
('pso172', 'Psoriasis or eczema', 'Psoriasis or eczema', 'PSO'),
('pvd168', 'Peripheral vascular disease', 'Peripheral vascular disease', 'PVD'),
('rhe174', "Rheumatoid arthritis, other inflammatory polyarthropathies & systematic connective tissue disorders", 'Rheumatoid arthritis', 'RHE'),
('scz175', "Schizophrenia (and related non-organic psychosis) or bipolar disorder", 'Schizophrenia', 'SCZ'),
('scz176', "Schizophrenia (and related non-organic psychosis) or bipolar disorder", 'Schizophrenia', 'SCZ'),
('sin149', 'Chronic sinusitis', 'Chronic sinusitis', 'SIN'),
('str130', 'Stroke & transient ischaemic attack', 'Stroke & transient ischaemic attack', 'STR'),
('thy179', 'Thyroid disorders', 'Thyroid disorders', 'THY');

ALTER TABLE .ccu030_${dt}_ltc_helper_table OWNER TO 

-- COMMAND ----------


