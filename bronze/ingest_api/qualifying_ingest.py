# Databricks notebook source
# create table race

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructField,StringType,StringType,IntegerType,StructType

input_schema = StructType(
    [
        StructField("qualifyId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("position", IntegerType()),
        StructField("q1", StringType()),
        StructField("q2", StringType()),
        StructField("q3", StringType())
    ]
)


qualifying_df = spark.read.schema(input_schema).json(f'/mnt/saf1racing/formulaoneproject/bronze/qualifying/2024-04-11/',multiLine='True')
qualifying_df.display()

# COMMAND ----------


