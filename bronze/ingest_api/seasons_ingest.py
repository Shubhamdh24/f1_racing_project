# Databricks notebook source
# create table seasons

from pyspark.sql.functions import col,explode
seasons_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formula-one-project/bronze/seasons',recursiveFileLookup=True)
seasons_df = seasons_df.withColumn('list',explode(col('MRData').SeasonTable.Seasons))
seasons_df = seasons_df.drop('MRData')
seasons_df = seasons_df.withColumn('season',col('list').season).withColumn('url',col('list').url)
seasons_df = seasons_df.drop('list')
seasons_df.display()

# COMMAND ----------

print(seasons_df.columns)
print(seasons_df.count())

# COMMAND ----------

seasons_df = seasons_df.select('season', 'url').distinct()
print(seasons_df.count())

# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the seasons delta table in silver storage account

# COMMAND ----------

seasons_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formula-one-project/silver/seasons')
