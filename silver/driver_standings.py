# Databricks notebook source
results_df = spark.read.parquet('/mnt/saf1racing/silver/results')
results_df = results_df.select('race_id','driver_id','constructor_id','points','position','position_text')

# COMMAND ----------

races_df = spark.read.parquet('/mnt/saf1racing/silver/races')
races_df = races_df.select('race_id','year')

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
results_races_df = results_df.join(races_df,'race_id','left')


# COMMAND ----------

total_points = results_races_df.groupBy('year','driver_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))


# COMMAND ----------

# writing driver_standing table
# for bbc standing section those columns required for creation those only took for creation of driver_standing table

total_points.write.parquet(path = '/mnt/saf1racing/gold/driver_standings',mode='overwrite')

# COMMAND ----------

driver_standings_df = spark.read.parquet('/mnt/saf1racing/gold/driver_standings')
driver_standings_df.display()

# COMMAND ----------

drivers_df = spark.read.parquet('/mnt/saf1racing/silver/drivers')
drivers_df.select('driver_id','full name')


# COMMAND ----------


qualifying_df = spark.read.parquet('/mnt/saf1racing/silver/qualifying')
qualifying_df = qualifying_df.select('driver_id','constructor_id').distinct().collect()
driver_constru_frm_result={}
for i in qualifying_df:
    driver_constru_frm_result[i.driver_id]=i. constructor_id

print(driver_constru_frm_result)





# COMMAND ----------

# Declaring the User Defined Function

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
def get_constructor_id(driver_id):
    return driver_constru_frm_result.get(driver_id)
get_constructor_id_udf = udf(get_constructor_id,IntegerType())




# COMMAND ----------

constructor_df = spark.read.parquet('/mnt/saf1racing/silver/constructors').select('constructor_id','Constructor_name')

# COMMAND ----------

drive_board_df = driver_standings_df.join(drivers_df,'driver_id','inner')
drive_board_df = drive_board_df.withColumn('constructor_id',get_constructor_id_udf(col('driver_id')))
# drive_board_df.display()
drive_board_df =drive_board_df.select(col('full name').alias('Driver'),'constructor_id','wins','total_points')
drive_board_df = drive_board_df.join(constructor_df,'constructor_id','inner').select('Driver',col('constructor_name').alias('Team'),'wins','total_points')
drive_board_df.display()

