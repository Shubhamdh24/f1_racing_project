# Databricks notebook source
# create table circuits

from pyspark.sql.functions import col,explode,replace

circuits_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formula-one-project/bronze/circuits',recursiveFileLookup=True)
circuits_df = circuits_df.withColumn('lst',explode(col('MRData').CircuitTable.Circuits))
circuits_df = circuits_df.drop('MRData')
circuits_df = circuits_df.withColumn('circuit_id',col('lst').circuitId).withColumn('name',col('lst').circuitName)\
    .withColumn('location',col('lst').Location.locality).withColumn('country',col('lst').Location.country).withColumn('lat',col('lst').Location.lat).withColumn('lng',col('lst').Location.long).withColumn('url',col('lst').url)

circuits_df = circuits_df.drop('lst')

circuits_df.display()

# COMMAND ----------

print(circuits_df.columns)
print(circuits_df.count())

# COMMAND ----------

# Find the distinct values from circuits_df

circuits_df = circuits_df.select('circuit_id', 'name', 'location', 'country', 'lat', 'lng', 'url').distinct()
circuits_df.count()

# COMMAND ----------

# Actual no. of columns = 7
# in api circuits no circuits_ref column present
# we don't need it further so we can directly write down the circuits delta table in silver storage account

# COMMAND ----------

circuits_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formula-one-project/silver/circuits')

# COMMAND ----------


