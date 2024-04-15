# Databricks notebook source
# create table qualifying

from pyspark.sql.functions import col,explode


qualifying_df = spark.read.json(['/mnt/saf1racing/formulaoneproject/bronze/qualifying/2024-04-14/','/mnt/saf1racing/formulaoneproject/bronze/qualifying/2024-04-15/'],multiLine='True')
qualifying_df= qualifying_df.withColumn('lst',explode(col('MRData').RaceTable.Races))
qualifying_df = qualifying_df.drop('MRData')
qualifying_df = qualifying_df.withColumn('quali_results',explode(col('lst').QualifyingResults))
qualifying_df = qualifying_df.withColumn('driver_id',col('quali_results').Driver.driverId).withColumn('constructor_id',col('quali_results').Constructor.constructorId).withColumn('number',col('quali_results').number).withColumn('position',col('quali_results').position).withColumn('q1',col('quali_results').Q1).withColumn('q2',col('quali_results').Q2).withColumn('q3',col('quali_results').Q3)
qualifying_df.display()

# COMMAND ----------

qualifying_df = qualifying_df.drop('MRData','lst','quali_results')
qualifying_df.display()


# COMMAND ----------


qualifying_df = qualifying_df.select('driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3').distinct()


# COMMAND ----------

qualifying_df.count()

# COMMAND ----------


