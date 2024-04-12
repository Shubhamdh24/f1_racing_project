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

lap_times_df = spark.read.csv(f'/mnt/saf1racing/formulaoneproject/bronze/lap_times/2024-04-11/',schema=input_schema)

lap_times_df.display()

# COMMAND ----------


