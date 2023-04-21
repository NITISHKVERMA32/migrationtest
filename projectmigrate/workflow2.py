# Databricks notebook source
# MAGIC %run
# MAGIC ./datasetup

# COMMAND ----------

patients=patients.distinct()
admissions=admissions.distinct()
doctors=doctors.distinct()

# COMMAND ----------

admissions.display()

# COMMAND ----------

all_patients=patients.join(admissions,patients.patient_id==admissions.patient_id,"inner")

# COMMAND ----------

all_doctors=patients.join(doctors,patients.patient_id==doctors.doctor_id,"inner")

# COMMAND ----------

# identify the duplicate columns in all_patients
duplicates = [col for col in all_patients.columns if all_patients.columns.count(col) > 1]

# drop the duplicate columns
all_patients = all_patients.drop(*duplicates)

# identify the duplicate columns in all_doctors
duplicates = [col for col in all_doctors.columns if all_doctors.columns.count(col) > 1]

# drop the duplicate columns
all_doctors = all_doctors.drop(*duplicates)

# write all_patients and all_doctors to CSV files
all_patients.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/result/all_patients.csv")
all_doctors.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/result/all_doctors.csv")
