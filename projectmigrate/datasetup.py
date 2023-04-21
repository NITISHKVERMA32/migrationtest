# Databricks notebook source
display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

# covid=spark.read.csv("dbfs:/FileStore/tables/covid.csv",header=True,inferSchema=True)  

# COMMAND ----------

patients=spark.read.csv("dbfs:/FileStore/tables/patients.csv",header=True,inferSchema=True)

# COMMAND ----------

admissions=spark.read.csv("dbfs:/FileStore/tables/admissions.csv",header=True,inferSchema=True)

# COMMAND ----------

doctors=spark.read.csv("dbfs:/FileStore/tables/doctors.csv",header=True,inferSchema=True)

# COMMAND ----------

province_names=spark.read.csv("dbfs:/FileStore/tables/province_names.csv",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
