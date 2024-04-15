# Databricks notebook source
# create table circuits

from pyspark.sql.functions import col,explode,replace

circuits_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/circuits/2024-04-14')
circuits_df = circuits_df.withColumn('lst',explode(col('MRData').CircuitTable.Circuits))
circuits_df = circuits_df.drop('MRData')
circuits_df = circuits_df.withColumn('circuit_id',col('lst').circuitId).withColumn('name',col('lst').circuitName)\
    .withColumn('location',col('lst').Location.locality).withColumn('country',col('lst').Location.country).withColumn('lat',col('lst').Location.lat).withColumn('lng',col('lst').Location.long).withColumn('url',col('lst').url)

circuits_df = circuits_df.drop('lst')

circuits_df.display()

# COMMAND ----------


