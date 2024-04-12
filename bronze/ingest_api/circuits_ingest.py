# Databricks notebook source
# create table circuits
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

input_schema = StructType([StructField('circuitId',IntegerType()),
                     StructField('circuitRef',StringType()),
                     StructField('name',StringType()),
                     StructField('location',StringType()),
                     StructField('country',StringType()),
                     StructField('lat',FloatType()),
                     StructField('lng',FloatType()),
                     StructField('alt',IntegerType()),
                     StructField('url',StringType())
]
                    )

df = spark.read.csv(f'/mnt/saf1racing/formulaoneproject/bronze/circuits/2024-04-11/',header=True,schema=input_schema)

df.display()

# COMMAND ----------


