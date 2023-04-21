# Databricks notebook source
display(dbutils.fs.ls("dbfs:/FileStore/tables/result"))

# COMMAND ----------

# spark.read.csv("dbfs:/FileStore/tables/result/covid_country_count_result.csv/",header=True,inferSchema=True).display()

# COMMAND ----------

# spark.read.csv("dbfs:/FileStore/tables/result/covid_weekday_count_result.csv/",header=True,inferSchema=True).display()

# COMMAND ----------

# spark.read.csv("dbfs:/FileStore/tables/result/covid_year_count_result.csv/",header=True,inferSchema=True).display()

# COMMAND ----------

spark.read.csv("dbfs:/FileStore/tables/result/all_patients.csv/",header=True,inferSchema=True).display()

# COMMAND ----------

spark.read.csv("dbfs:/FileStore/tables/result/all_doctors.csv/",header=True,inferSchema=True).display()

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/result",True)
