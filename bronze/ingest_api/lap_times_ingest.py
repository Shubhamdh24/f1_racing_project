# Databricks notebook source
# create table lap_times
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("position", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType())
    ]
)


# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import col,explode
lap_times_df = spark.read.json(f'/mnt/saf1racing/formulaoneproject/bronze/lap_times/2024-04-13/')
lap_times_df = lap_times_df.withColumn('list',col('MRData').RaceTable.Races.Laps)
# lap_times_df = lap_times_df.withColumn('races',explode(col('list')))
# lap_times_df = lap_times_df.withColumn('Laps',col('races').Laps)
lap_times_df = lap_times_df.withColumn('listlaps',explode(col('List')))
lap_times_df = lap_times_df.withColumn('listlaps1',explode(col('listlaps')))
lap_times_df = lap_times_df.withColumn('laps',col('listlaps1').number)
lap_times_df = lap_times_df.withColumn('timings',explode(col('listlaps1').Timings))
lap_times_df = lap_times_df.withColumn('driver_id',col('timings').driverId)
lap_times_df = lap_times_df.withColumn('position',col('timings').position)
lap_times_df = lap_times_df.withColumn('time',col('timings').time)
lap_times_df = lap_times_df.withColumn('Races',explode(col('MRData').RaceTable.Races.Laps))


lap_times_df.display()

# COMMAND ----------

print('test')
