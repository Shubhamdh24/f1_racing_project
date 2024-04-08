# Databricks notebook source
dbutils.widgets.text('file_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
file_name = dbutils.widgets.get('file_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

name_schema = StructType([
    StructField('forename',StringType()),
    StructField('surname',StringType())
])

# COMMAND ----------

input_schema = StructType([StructField('driverId',IntegerType()),
                     StructField('driverRef',StringType()),
                     StructField('number',IntegerType()),
                     StructField('code',StringType()),
                     StructField('name',name_schema),
                     StructField('dob',DateType()),
                     StructField('nationality',StringType()),
                     StructField('url',StringType())
]
                    )

# COMMAND ----------


df = spark.read.json(f'/mnt/saf1racing/bronze/{file_name}.json',schema=input_schema)

# COMMAND ----------

df = df.withColumn('full name',concat_ws(' ',df.name.forename,col('name.surname')))
df = df.drop(df.name)

# COMMAND ----------

df.display(Truncate=False)

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "driverId": "driver_id",
        "driverRef": "driver_ref",
        'url':'driver_url'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}/',header=True).display()
