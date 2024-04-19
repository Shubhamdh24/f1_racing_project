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


results_df = results_df.drop( 'races', 'results')


results_df.display()

# COMMAND ----------

print(results_df.columns)
print(results_df.count())

# COMMAND ----------

# Find the distinct values from results_df

results_df = results_df.select('MRData','driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'status').distinct()
results_df.count()

# COMMAND ----------

# Actual no. of columns = 18
# in api result_id, race_id,status_id is not present
# we need all the columns which are not present 


# COMMAND ----------

# space for retrive the columns
# race_id we created using year and round 
# here in results_df MRData we can see season and round from that we can generate race_id column here

from pyspark.sql.functions import concat

results_df = results_df.withColumn('season',col('MRData').RaceTable.season)
results_df = results_df.withColumn('round',col('MRData').RaceTable.round)
results_df = results_df.withColumn('race_id',concat('season','round'))
results_df = results_df.drop('MRData','season','round')

results_df.display()



# below line is for cross checking the no. of records after race_id column generation
print(results_df.count())

# COMMAND ----------

# We can get status_id from status_df
# as we have status column in results_df we can join results_df with status_df and retrive the status_id column 

status_df = spark.read.load('/mnt/saf1racing/formulaoneproject/silver/status')
results_df = results_df.join(status_df,'status','inner')


# COMMAND ----------

# we can generate result_id column by using monotonically_increasing_id function

from pyspark.sql.functions import monotonically_increasing_id

results_df = results_df.orderBy('driver_id','constructor_id','number')
results_df = results_df.withColumn('result_id',monotonically_increasing_id())
results_df.display()

# COMMAND ----------

results_df = results_df.select('result_id','race_id','driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'status_id').distinct()
print(results_df.count())
results_df.display()

# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the seasons delta table in silver storage account

# COMMAND ----------

results_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/results')
