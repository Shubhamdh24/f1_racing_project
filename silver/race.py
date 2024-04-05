# Databricks notebook source
drivers = spark.read.parquet('/mnt/saf1racing/silver/drivers')
drivers = drivers.select('driver_id','full name','number')

# COMMAND ----------

constructors = spark.read.parquet('/mnt/saf1racing/silver/constructors')
constructors = constructors.select('constructor_id','constructor_name')

# COMMAND ----------

results = spark.read.parquet('/mnt/saf1racing/silver/results')
results = results.select('race_id','driver_id','constructor_id','grid','fastest_lap_time','points')

# COMMAND ----------

races = spark.read.parquet('/mnt/saf1racing/silver/races')
races = races.select('race_id','time','year')

# COMMAND ----------

pit_stops = spark.read.parquet('/mnt/saf1racing/silver/pit_stops')
pit_stops = pit_stops.select('race_id','driver_id','stop')


# COMMAND ----------

from pyspark.sql.functions import col
df = drivers.join(results,'driver_id','left')
df = df.join(constructors,'constructor_id','left')
df = df.join(races,'race_id','left')
df = df.join(pit_stops,['race_id','driver_id'],'left')
df = df.select(col('full name').alias('Driver'),
               col('number').alias('Number'),
               col('constructor_name').alias('Team'),
               col('grid').alias('Grid'),
               col('stop').alias('Pits'),
               col('fastest_lap_time').alias('Fastest Lap'),
               col('time').alias('Race Time'),
               col('points').alias('Points'),
               col('year').alias('Year'),
               )



                                           

# COMMAND ----------

# first approach
# Get a driver name who has maximum point per year

from pyspark.sql.functions import sum,max,desc
driver_name = df.groupBy([col('Driver'),col('Year')]).agg(sum(col('Points')).alias('total_points_per_year')).select('Driver',col('Year').alias('d_year'),'total_points_per_year')
max_points = driver_name.groupBy(col('d_year')).agg(max(col('total_points_per_year')).alias('max_points_per_year')).select(col('d_year').alias('m_year'),'max_points_per_year')
final = driver_name.join(max_points,(driver_name.total_points_per_year == max_points.max_points_per_year) & (driver_name.d_year==max_points.m_year),'inner')
final = final.select('Driver',col('d_year').alias('Year'),col('max_points_per_year')).orderBy(col('Year').desc())
final.display()


# COMMAND ----------

# second approach
# Get a driver name who has maximum point per year

from pyspark.sql.functions import sum,max,desc,dense_rank
from pyspark.sql.window import Window

x = df.groupBy([col('Driver'),col('Year')]).agg(sum(col('Points')).alias('total_points_per_year')).select('Driver',col('Year').alias('d_year'),'total_points_per_year')

x = x.withColumn('rnk',dense_rank().over(Window.partitionBy('d_year').orderBy('d_year','Driver')))
x = x.filter(col('rnk') == 1)
x.display()

# COMMAND ----------

# Get a team name who has scored Max point each year


from pyspark.sql.functions import sum,max,desc
team_name = df.groupBy([col('Team'),col('Year')]).agg(sum(col('Points')).alias('total_points_per_year')).select('Team',col('Year').alias('d_year'),'total_points_per_year')
max_points = team_name.groupBy(col('d_year')).agg(max(col('total_points_per_year')).alias('max_points_per_year')).select(col('d_year').alias('m_year'),'max_points_per_year')
final = team_name.join(max_points,(team_name.total_points_per_year == max_points.max_points_per_year) & (team_name.d_year==max_points.m_year),'inner')
final = final.select('Team',col('d_year').alias('Year'),col('max_points_per_year')).orderBy(col('Year').desc())
final.display()

# COMMAND ----------

# second approach
# Get a Team name who has maximum point per year

from pyspark.sql.functions import sum,max,desc,dense_rank
from pyspark.sql.window import Window

x = df.groupBy([col('Team'),col('Year')]).agg(sum(col('Points')).alias('total_points_per_year')).select('Team',col('Year').alias('d_year'),'total_points_per_year')

x = x.withColumn('rnk',dense_rank().over(Window.partitionBy('d_year').orderBy('d_year','Team')))
x = x.filter(col('rnk') == 1)
x.display()
