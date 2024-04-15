# Databricks notebook source
# create table races

from pyspark.sql.functions import col,explode

race_df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/races/2024-04-14')
race_df = race_df.withColumn('list',explode(col('MRData').RaceTable.Races))
race_df = race_df.drop('MRData')
race_df = race_df.withColumn('year',col('list').season).withColumn('round',col('list').round).withColumn('circuit_id',col('list').Circuit.circuitId).withColumn('name',col('list').raceName).withColumn('date',col('list').date)
race_df = race_df.drop('list')
race_df.display()

# COMMAND ----------


