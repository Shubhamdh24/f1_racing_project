# Databricks notebook source
# create table constructor
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
input_schema = StructType([StructField('constructorId',IntegerType()),
                     StructField('constructorRef',StringType()),
                     StructField('name',StringType()),
                     StructField('nationality',StringType()),
                     StructField('url',StringType())
]
                    )

constructor_df = spark.read.json(f'/mnt/saf1racing/formulaoneproject/bronze/constructors/2024-04-11/',schema=input_schema)
constructor_df.display()

# COMMAND ----------


