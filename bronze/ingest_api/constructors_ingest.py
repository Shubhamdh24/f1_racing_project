# Databricks notebook source
# create table constructors

from pyspark.sql.functions import col,explode,replace
constructors_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/constructors',recursiveFileLookup=True)
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

print(constructors_df.columns)
print(constructors_df.count())

# COMMAND ----------

constructors_df = constructors_df.select('constructor_id', 'constructor_ref', 'name', 'nationality', 'url').distinct()
print(constructors_df.count())

# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the constructors delta table in silver storage account

# COMMAND ----------

constructors_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/constructors')
