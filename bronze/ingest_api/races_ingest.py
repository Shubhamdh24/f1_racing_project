# Databricks notebook source
# create table race

from pyspark.sql.functions import col,explode
race_df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/races/2024-04-12')
race_df = race_df.withColumn('list',explode(col('MRData').RaceTable.Races))
race_df = race_df.drop('MRData')
race_df = race_df.withColumn('season',col('list').season).withColumn('round',col('list').round).withColumn('date',col('list').date)\
    .withColumn('race_name',col('list').raceName).withColumn('circuit_id',col('list').Circuit.circuitId)
race_df = race_df.drop('list')
race_df.display()
