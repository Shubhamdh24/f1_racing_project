# Databricks notebook source
dbutils.notebook.run('../bronze/ingest_circuits_file',0,{'file_name':'circuits','container_name':'silver'})


# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_constructors_file',0,{'file_name':'constructors','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_drivers_file',0,{'file_name':'drivers','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_multiple_lap_times_file',0,{'folder_name':'lap_times','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_multiple_qualifying_file',0,{'folder_name':'qualifying','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_pit_stops_file',0,{'file_name':'pit_stops','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_races_file',0,{'file_name':'races','container_name':'silver'})

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_results_file',0,{'file_name':'results','container_name':'silver'})
