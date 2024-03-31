# Databricks notebook source
# MAGIC %run ../bronze

# COMMAND ----------

dbutils.notebook.run('../bronze/ingest_circuits_file',0,{'file name':''})

