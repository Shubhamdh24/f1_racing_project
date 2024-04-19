# Databricks notebook source
# create table races

from pyspark.sql.functions import col,explode

race_df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/races',recursiveFileLookup=True)
race_df = race_df.withColumn('list',explode(col('MRData').RaceTable.Races))
race_df = race_df.drop('MRData')
race_df = race_df.withColumn('year',col('list').season).withColumn('round',col('list').round).withColumn('circuit_id',col('list').Circuit.circuitId).withColumn('name',col('list').raceName).withColumn('date',col('list').date)
race_df = race_df.drop('list')
race_df.display()

# COMMAND ----------

print(race_df.columns)
print(race_df.count())

# COMMAND ----------

race_df = race_df.select('year', 'round', 'circuit_id', 'name', 'date').distinct()
print(race_df.count())

# COMMAND ----------

# there are total 8 columns but in api 3 columns are not present 
# from that 3 ('race_id',time','url')
# 'race_id' is essential
# so first write that race_id column and then we can write the races delta table in silver 

# COMMAND ----------

# space for writing logic to add race_id column

from pyspark.sql.functions import concat

race_df = race_df.orderBy('year','round')
race_df = race_df.withColumn('race_id',concat('year','round'))
race_df = race_df.select('race_id','year', 'round', 'circuit_id', 'name', 'date')
race_df.display()


# COMMAND ----------



# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the races delta table in silver storage account

# COMMAND ----------

race_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/races')
