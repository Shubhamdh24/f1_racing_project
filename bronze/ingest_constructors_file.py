# Databricks notebook source
dbutils.widgets.text('file_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
file_name = dbutils.widgets.get('file_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

input_schema = StructType([StructField('constructorId',IntegerType()),
                     StructField('constructorRef',StringType()),
                     StructField('name',StringType()),
                     StructField('nationality',StringType()),
                     StructField('url',StringType())
]
                    )

# COMMAND ----------


df = spark.read.json(f'/mnt/saf1racing/bronze/{file_name}.json',schema=input_schema)

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "constructorId": "constructor_id",
        "constructorRef": "constructor_ref",
        "name": "constructor_name",
        "nationality": "constructor_nationality",
        'url':'constructor_url'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}.parquet',header=True).display(Truncate=False)
