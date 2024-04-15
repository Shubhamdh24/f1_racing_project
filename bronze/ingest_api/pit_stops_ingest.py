# Databricks notebook source
# create table pit_stops
from pyspark.sql.functions import col,explode,regexp_replace

pit_stops_df = spark.read.json(['/mnt/saf1racing/formulaoneproject/bronze/pit_stops/2024-04-15','/mnt/saf1racing/formulaoneproject/bronze/pit_stops/2024-04-14'],multiLine=True)
pit_stops_df = pit_stops_df.withColumn('list',explode(col('MRData').RaceTable.Races))
pit_stops_df = pit_stops_df.withColumn('pit_stops',explode(col('list').PitStops))
pit_stops_df = pit_stops_df.withColumn('driver_id',col('pit_stops').driverId)\
                            .withColumn('stop',col('pit_stops').stop)\
                            .withColumn('lap',col('pit_stops').lap)\
                            .withColumn('time',col('pit_stops').time)\
                            .withColumn('duration',col('pit_stops').duration)
pit_stops_df = pit_stops_df.drop('MRData','pit_stops','list')

pit_stops_df.display()



# COMMAND ----------


pit_stops_df = spark.read.option('multiline',True).schema(input_schema).json(f'/mnt/saf1racing/formulaoneproject/bronze/pit_stops/')


pit_stops_df.display()
