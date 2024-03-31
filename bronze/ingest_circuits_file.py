# Databricks notebook source
dbutils.widgets.text('file_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
file_name = dbutils.widgets.get('file_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

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

# COMMAND ----------

df = spark.read.csv(f'/mnt/saf1racing/bronze/{file_name}.csv',header='true',schema=input_schema)

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "circuitId": "circuit_id",
        "circuitRef": "circuit_ref",
        "name": "circuit_name",
        'location': 'circuit_location',
        'çountry':'circuit_country',
        'lat':'circuit_lat',
        'lng':'circuit_lng',
        'alt':'çircuit_alt',
        'url':'circuit_url'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}.parquet',header=True).display()
