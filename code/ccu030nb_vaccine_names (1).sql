-- Databricks notebook source
-- MAGIC %python
-- MAGIC dt = dbutils.widgets.get("dt")
-- MAGIC print(dt)

-- COMMAND ----------

drop table if exists .ccu030_${dt}_vaccine_names;
CREATE TABLE .ccu030_${dt}_vaccine_names (VACCINE_PRODUCT_CODE LONG, vaccine_name STRING) STORED AS ORC;
INSERT INTO .ccu030_${dt}_vaccine_names VALUES 
(39114911000001105, "astrazeneca"),
(39115011000001105, "astrazeneca"),
(39115111000001106, "astrazeneca"),
(39115611000001103, "pfizer"),
(39115711000001107, "pfizer"),
(39230211000001104, "janssen"),
(39326911000001101, "moderna"),
(39373511000001104, "valneva"),
(39375411000001104, "moderna"),
(39473011000001103, "novavax"),
(39826711000001101, "medicago"),
(40306411000001101, "sinovac"),
(40331611000001100, "sinopharm"),
(40332311000001101, "covaxin"),
(40348011000001102, "covashield"),
(40384611000001108, "pfizer"),
(40402411000001109, "astrazeneca")

-- COMMAND ----------


