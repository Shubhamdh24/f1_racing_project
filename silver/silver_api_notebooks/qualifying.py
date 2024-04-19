# Databricks notebook source
from pyspark.sql.functions import concat_ws,col
drivers = spark.read.load('/mnt/saf1racing/formulaoneproject/silver/drivers')
drivers = drivers.select('driver_id',concat_ws(' ',col('forename'),col('surname')).alias('full name'),'number')

# COMMAND ----------

from pyspark.sql.functions import concat_ws,col
constructors = spark.read.load('/mnt/saf1racing/formulaoneproject/silver/constructors')
constructors = constructors.select('constructor_id',col('name').alias('constructor_name'))

# COMMAND ----------

qualifying = spark.read.load('/mnt/saf1racing/formulaoneproject/silver/qualifying')
qualifying = qualifying.select('race_id','driver_id','constructor_id','q1','q2','q3')
qualifying.display()

# COMMAND ----------

races = spark.read.load('/mnt/saf1racing/formulaoneproject/silver/races')
races = races.select('race_id','year','date')

# COMMAND ----------

from pyspark.sql.functions import col,asc
final_qualifying = drivers.join(qualifying,'driver_id','left')
final_qualifying = final_qualifying.join(constructors,'constructor_id','left')
final_qualifying = final_qualifying.join(races,'race_id','left')

final_qualifying = final_qualifying.select(col('full name').alias('Driver'),
                                           col('number').alias('Number'),
                                           col('constructor_name').alias('Team'),
                                           col('q1').alias('Qualifying1'),
                                           col('q2').alias('Qualifying2'),
                                           col('q3').alias('Qualifying3'),
                                           col('year'),col('date'))


final_qualifying.filter((col('year')=='2024')).display()
                                        

                                           

# COMMAND ----------

# write bbc_qualifying results in gold 

final_qualifying.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/gold/bbc_qualifying_results')



# COMMAND ----------

# Get the driver name and Team name who covered the qualifying race in minimum time (q1)?

from pyspark.sql.functions import col,min,regexp_replace
import re

df = final_qualifying.withColumn('Qualifying1',regexp_replace(col('Qualifying1'),'[\\\\N]',''))
df = df.filter((col('Qualifying1').isNotNull()) & (col('Qualifying1')!=''))
min_time = df.agg(min(col('Qualifying1').alias('min_value'))).collect()[0][0]
df = df.filter(col('Qualifying1')==min_time)
df.display()


