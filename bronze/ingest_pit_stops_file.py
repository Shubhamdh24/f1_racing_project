# Databricks notebook source
dbutils.widgets.text('file_name','')
dbutils.widgets.text('container_name','',label='where data frame will be written')
file_name = dbutils.widgets.get('file_name')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

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

# COMMAND ----------


df = spark.read.option('multiline',True).schema(input_schema).json(f'/mnt/saf1racing/bronze/{file_name}.json')

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

df.display()

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}.parquet',header=True).display()

# COMMAND ----------

# dbutils.fs.ls('/mnt/saf1racing/silver')
# dbutils.fs.rm('/mnt/saf1racing/silver/circuits.parquet',recurse=True)

# COMMAND ----------


