# Databricks notebook source
# MAGIC %run
# MAGIC ./datasetup

# COMMAND ----------

c=covid.count()
print(c)

# COMMAND ----------

covid.limit(1).display()

# COMMAND ----------

covid_year_count_result=covid.groupBy(col("Year")).count().orderBy(col("count").desc())

# COMMAND ----------

covid_country_count_result=covid.groupBy(col("Country")).count().orderBy(col("count").desc())


# COMMAND ----------

covid_weekday_count_result=covid.groupBy(col("Weekday")).count().orderBy(col("count").desc())

# COMMAND ----------

covid_year_count_result.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/result/covid_year_count_result.csv")
covid_country_count_result.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/result/covid_country_count_result.csv")
covid_weekday_count_result.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/result/covid_weekday_count_result.csv")
