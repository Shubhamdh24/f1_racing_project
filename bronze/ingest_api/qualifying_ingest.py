# Databricks notebook source
# create table qualifying

from pyspark.sql.functions import col,explode


qualifying_df = spark.read.json('/mnt/saf1racing/formula-one-project/bronze/qualifying',multiLine='True',recursiveFileLookup=True)

qualifying_df= qualifying_df.withColumn('lst',explode(col('MRData').RaceTable.Races))
qualifying_df = qualifying_df.withColumn('quali_results',explode(col('lst').QualifyingResults))
qualifying_df = qualifying_df.withColumn('driver_id',col('quali_results').Driver.driverId).withColumn('constructor_id',col('quali_results').Constructor.constructorId).withColumn('number',col('quali_results').number).withColumn('position',col('quali_results').position).withColumn('q1',col('quali_results').Q1).withColumn('q2',col('quali_results').Q2).withColumn('q3',col('quali_results').Q3)

qualifying_df.display()

# COMMAND ----------

qualifying_df = qualifying_df.drop('lst','quali_results')
qualifying_df.display()


# COMMAND ----------

print(qualifying_df.columns)
print(qualifying_df.count())

# COMMAND ----------


qualifying_df = qualifying_df.select('MRData','driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3').distinct()
print(qualifying_df.count())

# COMMAND ----------

# there are total 9 columns but in api 2 columns are not present 
# from that 2 ('qualify_id','race_id')
# both those are essential
# add that race_id column in qualifying 
# and then write qullfy_id column to qualifying_df
# and then we can write the qualifying_df delta table in silver


# COMMAND ----------

# race_id we created using year and round 
# here in qulifying MRData we can see season and round from that we can generate race_id column here

from pyspark.sql.functions import concat

qualifying_df = qualifying_df.withColumn('season',col('MRData').RaceTable.season)
qualifying_df = qualifying_df.withColumn('round',col('MRData').RaceTable.round)
qualifying_df = qualifying_df.withColumn('race_id',concat('season','round'))
qualifying_df = qualifying_df.drop('MRData','season','round')
qualifying_df = qualifying_df.select('race_id','driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3')
qualifying_df.display()



# below line is for cross checking the no. of records after race_id column generation
print(qualifying_df.count())



# COMMAND ----------

# space to write qualifying_id column in qualifying_df

from pyspark.sql.functions import monotonically_increasing_id

# we can directlry add qualifying_id column with values like 1,2,3,4,5........
# to achieve this we use sdf_with_sequential_id(x, id = "id", from = 1L) function
# where x is data_frame id mean column name you have to add and from mean starting value

qualifying_df = qualifying_df.orderBy('driver_id','constructor_id','number')
qualifying_df =qualifying_df.withColumn('qualify_id',monotonically_increasing_id())

qualifying_df = qualifying_df.select('qualify_id','race_id','driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3')


qualifying_df.display()

# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the qualifying_df delta table in silver storage account

# COMMAND ----------

qualifying_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formula-one-project/silver/qualifying')


