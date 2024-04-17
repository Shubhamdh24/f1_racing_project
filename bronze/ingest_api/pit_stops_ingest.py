# Databricks notebook source
# create table pit_stops
from pyspark.sql.functions import col,explode,regexp_replace

pit_stops_df = spark.read.json(['/mnt/saf1racing/formulaoneproject/bronze/pit_stops/2024-04-15','/mnt/saf1racing/formulaoneproject/bronze/pit_stops/2024-04-14'],multiLine=True)
pit_stops_df = pit_stops_df.withColumn('list',explode(col('MRData').RaceTable.Races))
pit_stops_df = pit_stops_df.withColumn('pit_stops',explode(col('list').PitStops))
pit_stops_df = pit_stops_df.withColumn('driver_id',col('pit_stops').driverId)\
                            .withColumn('stop',col('pit_stops').stop)\
                            .withColumn('lap',col('pit_stops').lap)\
                            .withColumn('time',col('pit_stops').time)\
                            .withColumn('duration',col('pit_stops').duration)
pit_stops_df = pit_stops_df.drop('list')

pit_stops_df.display()



# COMMAND ----------

print(pit_stops_df.columns)
print(pit_stops_df.count())

# COMMAND ----------

pit_stops_df = pit_stops_df.select('MRData','pit_stops','driver_id', 'stop', 'lap', 'time', 'duration').distinct()
print(pit_stops_df.count())

# COMMAND ----------

# there are total 7 columns but in api 2 columns are not present 
# from that 2 ('race_id',milliseconds')
# adding both columns in df
# and then we can write the pit_stops delta table in silver 

# COMMAND ----------

# space to add millisecond column in pit_stops_df
# we have duration in seconds we can convert it to millisecond as we know 1 second = 1000 milliseconds
# add milliseond column

pit_stops_df = pit_stops_df.withColumn('milliseconds',(col('pit_stops').duration)*1000)
pit_stops_df.display()

# COMMAND ----------

# race_id we created using year and round 
# here in pit_stops MRData we can see season and round from that we can generate race_id column here

from pyspark.sql.functions import concat

pit_stops_df = pit_stops_df.withColumn('season',col('MRData').RaceTable.season)
pit_stops_df = pit_stops_df.withColumn('round',col('MRData').RaceTable.round)
pit_stops_df = pit_stops_df.withColumn('race_id',concat('season','round'))
pit_stops_df = pit_stops_df.drop('pit_stops','MRData','season','round')
pit_stops_df = pit_stops_df.select('race_id','driver_id', 'stop', 'lap', 'time', 'duration')
pit_stops_df.display()



# below line is for cross checking the no. of records after race_id column generation
print(pit_stops_df.count())


# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the pit_stops delta table in silver storage account

pit_stops_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/pit_stops')

# COMMAND ----------


