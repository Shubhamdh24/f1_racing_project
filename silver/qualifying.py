# Databricks notebook source
drivers = spark.read.parquet('/mnt/saf1racing/silver/drivers')
drivers = drivers.select('driver_id','full name','number')

# COMMAND ----------

constructors = spark.read.parquet('/mnt/saf1racing/silver/constructors')
constructors = constructors.select('constructor_id','constructor_name')

# COMMAND ----------

qualifying = spark.read.parquet('/mnt/saf1racing/silver/qualifying')
qualifying = qualifying.select('driver_id','constructor_id','q1','q2','q3')

# COMMAND ----------

from pyspark.sql.functions import col
final_qualifying = drivers.join(qualifying,'driver_id','left')
final_qualifying = final_qualifying.join(constructors,'constructor_id','left')
final_qualifying = final_qualifying.orderBy(final_qualifying.q1.asc_nulls_last()).select(col('full name').alias('Driver'),

                                           col('number').alias('Number'),
                                           col('constructor_name').alias('Team'),
                                           col('q1').alias('Qualifying1'),
                                           col('q2').alias('Qualifying2'),
                                           col('q3').alias('Qualifying3'))

df = final_qualifying.filter(col('Driver')=='Jenson Button').display()
                                        

                                           

# COMMAND ----------

# Get the driver name and Team name who covered the qualifying race in minimum time (q1)?

from pyspark.sql.functions import col,min,regexp_replace
import re

df = final_qualifying.withColumn('Qualifying1',regexp_replace(col('Qualifying1'),'[\\\\N]',''))
df = df.filter((col('Qualifying1').isNotNull()) & (col('Qualifying1')!=''))
min_time = df.agg(min(col('Qualifying1').alias('min_value'))).collect()[0][0]
df = df.filter(col('Qualifying1')==min_time)
df.display()



# COMMAND ----------

# qualifying = spark.read.parquet('/mnt/saf1racing/silver/qualifying').filter((col('Driver')=='Jenson Button')&(col('q1')=='1:41.119')).display()

# COMMAND ----------

# from pyspark.sql.types import StructType,StructField,StringType
# data = [(r'\\n'),(r'\\N')]
# schema = StructType(
#     [StructField('id',StringType())
#      ]
#     )
# df = spark.createDataFrame(data,schema)
# df.display()




# COMMAND ----------


