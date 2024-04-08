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
        StructField("resultId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("grid", IntegerType()),
        StructField("position", IntegerType()),
        StructField("positionText", StringType()),
        StructField("positionOrder", IntegerType()),
        StructField("points", IntegerType()),
        StructField("laps", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
        StructField("fastestLap", IntegerType()),
        StructField("rank", IntegerType()),
        StructField("fastestLapTime", StringType()),
        StructField("fastestLapSpeed", FloatType()),
        StructField("statusId", IntegerType())

    ]
)

# COMMAND ----------


df = spark.read.schema(input_schema).json(f'/mnt/saf1racing/bronze/{file_name}.json')

# COMMAND ----------

df = df.withColumnsRenamed(
    colsMap={
        "resultId":"result_id",
        "raceId" :"race_id",
        'driverId':'driver_id',
        'constructorId':'constructor_id',
        'positionText': 'position_text',
        'positionOrder': 'position_order',
        'fastestLap': 'fastest_lap',
        'fastestLapTime': 'fastest_lap_time',
        'fastestLapSpeed': 'fastest_lap_speed',
        'statusId': 'status_id'
    }
)

# COMMAND ----------

df = add_ingestion_date(df)

# COMMAND ----------

df = write_file(df,container_name,file_name)

# COMMAND ----------

df = spark.read.parquet(f'/mnt/saf1racing/{container_name}/{file_name}/',header=True)
display(df.limit(4))

# COMMAND ----------



# COMMAND ----------

print('jeep')
