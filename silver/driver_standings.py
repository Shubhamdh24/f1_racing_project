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

total_points = results_races_df.groupBy('year','driver_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

rdf = total_points.join(results_races_df,(total_points.driver_id == results_races_df.driver_id) & (total_points.wins==results_races_df.position)).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create view hive_metastore.f1_racing_project.resultview as
# MAGIC select * from parquet.`/mnt/saf1racing/silver/results`;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC use database f1_racing_project;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from resultview

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view racesview
# MAGIC as
# MAGIC select race_id,year from parquet.`/mnt/saf1racing/silver/races`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from racesview

# COMMAND ----------


