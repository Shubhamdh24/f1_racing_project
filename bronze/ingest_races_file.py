# Databricks notebook source
dbutils.widgets.text('file_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
file_name = dbutils.widgets.get('file_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

input_schema = StructType([StructField('raceId',IntegerType()),
                     StructField('year',IntegerType()),
                     StructField('round',IntegerType()),
                     StructField('circuitId',IntegerType()),
                     StructField('name',StringType()),
                     StructField('date',DateType()),
                     StructField('time',StringType()),
                     StructField('url',StringType())
]
                    )

# COMMAND ----------


df = spark.read.csv(f'/mnt/saf1racing/bronze/{file_name}.csv',header='true',schema=input_schema)

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "raceId": "race_id",
        "circuitId": "circuit_id",
        "race_name": "circuit_name",
        'url':'race_url'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}.parquet',header=True).display()
