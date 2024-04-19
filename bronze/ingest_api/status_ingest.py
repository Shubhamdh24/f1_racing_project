# Databricks notebook source
# create table status

from pyspark.sql.functions import col,explode,replace

status_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/status',recursiveFileLookup=True)
status_df = status_df.withColumn('lst',explode(col('MRData').StatusTable.Status))
status_df = status_df.drop('MRData')
status_df = status_df.withColumn('status_id',col('lst').statusId).withColumn('status',col('lst').status)


status_df = status_df.drop('lst')

status_df.display()

# COMMAND ----------

print(status_df.columns)
print(status_df.count())

# COMMAND ----------

# Find the distinct values from status_df

status_df = status_df.select('status_id', 'status').distinct()
status_df.count()

# COMMAND ----------

#  we can directly write down the status  delta table in silver storage account

# COMMAND ----------

status_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/status')

# COMMAND ----------


