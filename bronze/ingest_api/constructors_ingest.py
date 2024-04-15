# Databricks notebook source
# create table constructors

from pyspark.sql.functions import col,explode,replace
constructors_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/constructors/2024-04-14')
constructors_df = constructors_df.withColumn('lst', explode(col('MRData').ConstructorTable.Constructors))
constructors_df = constructors_df.drop('MRData')
constructors_df = constructors_df.withColumn('constructor_id', col('lst').constructorId)\
                                 .withColumn('constructor_ref', col('lst').constructorId)\
                                 .withColumn('name',col('lst.name'))\
                                 .withColumn('nationality', col('lst').nationality)\
                                 .withColumn('url', col('lst').url)
constructors_df = constructors_df.drop(col('lst'))
constructors_df.display()

# COMMAND ----------

constructors_df = constructors_df.drop(col('lst'))
constructors_df.display()

# COMMAND ----------


