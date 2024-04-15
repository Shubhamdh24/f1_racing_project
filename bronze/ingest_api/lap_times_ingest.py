# Databricks notebook source

from pyspark.sql.functions import col,explode
lap_times_df = spark.read.json(path=['/mnt/saf1racing/formulaoneproject/bronze/lap_times/2024-04-14','/mnt/saf1racing/formulaoneproject/bronze/lap_times/2024-04-15'],multiLine=True)
lap_times_df = lap_times_df.withColumn('list',explode(col('MRData').RaceTable.Races))
lap_times_df = lap_times_df.withColumn('list1',explode(col('list').Laps))
lap_times_df = lap_times_df.withColumn('list2',explode(col('list1').Timings))



lap_times_df.display()

# COMMAND ----------

lap_times_df = lap_times_df.withColumn('driver_id',col('list2').driverId)\
                            .withColumn('lap',col('list1').number)\
                            .withColumn('position',col('list2').position)\
                            .withColumn('time',col('list2').time)
lap_times_df = lap_times_df.drop('MRData','list','list1','list2')
lap_times_df.display()

# COMMAND ----------


print(lap_times_df.count())
lap_times_df = lap_times_df.select('driver_id', 'lap', 'position', 'time').distinct()
lap_times_df.count()


# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------


