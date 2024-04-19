# Databricks notebook source
# create table results

from pyspark.sql.functions import col,explode

results_df = spark.read.json('/mnt/saf1racing/formulaoneproject/bronze/results/',recursiveFileLookup=True,multiLine=True)


results_df = results_df.withColumn('races',explode(col('MRData').RaceTable.Races))
results_df = results_df.withColumn('results',explode(col('races').Results))

results_df = results_df.withColumn('driver_id',col('results').Driver.DriverId)
results_df = results_df.withColumn('constructor_id',col('results').Constructor.constructorId)
results_df = results_df.withColumn('number',col('results').number)
results_df = results_df.withColumn('grid',col('results').grid)
results_df = results_df.withColumn('position',col('results').position)
results_df = results_df.withColumn('position_text',col('results').positionText)
results_df = results_df.withColumn('points',col('results').points)
results_df = results_df.withColumn('laps',col('results').Laps)
results_df = results_df.withColumn('time',col('results').Time.time)
results_df = results_df.withColumn('milliseconds',col('results').Time.millis)
results_df = results_df.withColumn('fastest_lap',col('results').FastestLap.lap)
results_df = results_df.withColumn('rank',col('results').FastestLap.rank)
results_df = results_df.withColumn('fastest_lap_time',col('results').FastestLap.Time.time)
results_df = results_df.withColumn('fastest_lap_speed',col('results').FastestLap.AverageSpeed.speed)
results_df = results_df.withColumn('status',col('results').status)


results_df = results_df.drop('MRData', 'races', 'results')


results_df.display()

# COMMAND ----------

print(results_df.columns)
print(results_df.count())

# COMMAND ----------

# Find the distinct values from results_df

results_df = results_df.select('driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'status').distinct()
results_df.count()

# COMMAND ----------

# Actual no. of columns = 18
# in api result_id, race_id,status_id is not present
# we need all the columns which are not present 
# In api status column is there we can find statuse_id column by joining results_df with status_df
# same we can do for race_id 
# we can add new result_id column here

# COMMAND ----------

# space for retrive the columns

# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the seasons delta table in silver storage account
