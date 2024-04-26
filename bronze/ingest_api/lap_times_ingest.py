# Databricks notebook source
from pyspark.sql.functions import col,explode
lap_times_df = spark.read.json(path='/mnt/saf1racing/formula-one-project/bronze/lap_times',multiLine=True,recursiveFileLookup=True)
lap_times_df = lap_times_df.withColumn('list',explode(col('MRData').RaceTable.Races))
lap_times_df = lap_times_df.withColumn('list1',explode(col('list').Laps))
lap_times_df = lap_times_df.withColumn('list2',explode(col('list1').Timings))



lap_times_df.display()

# COMMAND ----------


# 'race_id' is essential
# so first add that race_id column 

# COMMAND ----------

# race_id we created using year and round 
# here in lap_times MRData we can see season and round from that we can generate race_id column here

from pyspark.sql.functions import concat

lap_times_df = lap_times_df.withColumn('season',col('MRData').RaceTable.season)
lap_times_df = lap_times_df.withColumn('round',col('MRData').RaceTable.round)
lap_times_df = lap_times_df.withColumn('race_id',concat('season','round'))
lap_times_df = lap_times_df.drop('season','round')

lap_times_df.display()



# below line is for cross checking the no. of records after race_id column generation
print(lap_times_df.count())

# COMMAND ----------

lap_times_df = lap_times_df.withColumn('driver_id',col('list2').driverId)\
                            .withColumn('lap',col('list1').number)\
                            .withColumn('position',col('list2').position)\
                            .withColumn('time',col('list2').time)
lap_times_df = lap_times_df.drop('MRData','list','list1','list2')
lap_times_df.display()

# COMMAND ----------

print(lap_times_df.columns)
print(lap_times_df.count())

# COMMAND ----------



lap_times_df = lap_times_df.select('race_id','driver_id', 'lap', 'position', 'time').distinct()
lap_times_df.count()


# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the lap_times delta table in silver storage account

lap_times_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formula-one-project/silver/lap_times')
