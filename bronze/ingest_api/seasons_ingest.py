# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# create table seasons

from pyspark.sql.functions import col,explode
seasons_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/seasons/2024-04-12')
seasons_df = seasons_df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))
seasons_df = seasons_df.drop('MRData')
seasons_df = seasons_df.withColumn('season',col('list').season).withColumn('url',col('list').url)
seasons_df = seasons_df.drop('list')
seasons_df.display()

# COMMAND ----------


