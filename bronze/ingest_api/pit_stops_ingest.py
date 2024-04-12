# Databricks notebook source
# create table pit_stops
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("stop", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("time", StringType()),
        StructField("duration", FloatType()),
        StructField("milliseconds", IntegerType())
    ]
)

pit_stops_df = spark.read.option('multiline',True).schema(input_schema).json(f'/mnt/saf1racing/formulaoneproject/bronze/pit_stops/2024-04-11/')


pit_stops_df.display()

# COMMAND ----------


