# Databricks notebook source
dbutils.widgets.text('folder_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
folder_name = dbutils.widgets.get('folder_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

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


df = spark.read.schema(input_schema).csv(f'/mnt/saf1racing/bronze/{folder_name}/*')

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "raceId" :"race_id",
        'driverId':'driver_id'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,folder_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{folder_name}.parquet',header=True)
display(df.limit(4))
