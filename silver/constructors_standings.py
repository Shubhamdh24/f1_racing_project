# Databricks notebook source
results_df = spark.read.parquet('/mnt/saf1racing/silver/results')
results_df = results_df.select('race_id','driver_id','constructor_id','points','position','position_text')

# COMMAND ----------

races_df = spark.read.parquet('/mnt/saf1racing/silver/races')
races_df = races_df.select('race_id','year')

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
results_races_df = results_df.join(races_df,'race_id','left')


# COMMAND ----------

total_points = results_races_df.groupBy('year','constructor_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))


# COMMAND ----------

# writing constructors_standing table
# for bbc standing section those columns required for creation those only took for creation of driver_standing table

total_points.write.parquet(path = '/mnt/saf1racing/gold/constructors_standings',mode='overwrite')

# COMMAND ----------

constructors_stand_df = spark.read.parquet('/mnt/saf1racing/gold/constructors_standings')


# COMMAND ----------

constructors_df = spark.read.parquet('/mnt/saf1racing/silver/constructors').select('constructor_id','constructor_name')

# COMMAND ----------

constructors_stand_df = constructors_stand_df.join(constructors_df,'constructor_id','inner').select(col('constructor_name').alias('Team'),'wins','total_points').display()

# COMMAND ----------


