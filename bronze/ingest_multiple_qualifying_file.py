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

# COMMAND ----------


df = spark.read.schema(input_schema).json(f'/mnt/saf1racing/bronze/{folder_name}/*',multiLine='True')

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "raceId" :"race_id",
        'driverId':'driver_id',
        "qualifyId":'qualifying_id',
        "constructorId":'constructor_id'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,folder_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{folder_name}/',header=True)
display(df.limit(4))
