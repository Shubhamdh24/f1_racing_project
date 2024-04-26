# Databricks notebook source
results_df = spark.read.load('/mnt/saf1racing/formula-one-project/silver/results')
driver_standings_df = results_df.select('race_id','driver_id','points','position','position_text','constructor_id')

# COMMAND ----------

races_df = spark.read.load('/mnt/saf1racing/formula-one-project/silver/races')
races_df = races_df.select('race_id','year')

# COMMAND ----------

from pyspark.sql.functions import count,sum,when,col
driver_standings_with_year = driver_standings_df.join(races_df,'race_id','inner').select('driver_id','year','constructor_id')
driver_standings_with_year.display()

# COMMAND ----------

from pyspark.sql.functions import count,sum,when,col
bbc_driver_standing = driver_standings_df.join(races_df,'race_id','inner')
bbc_driver_standing = bbc_driver_standing.groupBy('year','driver_id').agg(count(when(col('position')==1,True)).alias('Wins'),sum(col('points')).alias('Points'))
bbc_driver_standing.display()

# COMMAND ----------


bbc_driver_standing1 = bbc_driver_standing.join(driver_standings_with_year,['year','driver_id'],'left').distinct()


# COMMAND ----------

from pyspark.sql.functions import concat_ws
drivers_df = spark.read.load('/mnt/saf1racing/formula-one-project/silver/drivers')
drivers_df = drivers_df.select('driver_id',concat_ws(' ',col('forename'),col('surname')).alias('full name'))
drivers_df.display()

# COMMAND ----------

bbc_driver_standing1 = bbc_driver_standing1.join(drivers_df,'driver_id','inner')
bbc_driver_standing1.display()

# COMMAND ----------

from pyspark.sql.functions import desc
constructor_df = spark.read.load('/mnt/saf1racing/formula-one-project/silver/constructors').select('constructor_id',col('name').alias('Team'))
bbc_driver_standing2 = bbc_driver_standing1.join(constructor_df,'constructor_id','inner')
bbc_driver_standing2 = bbc_driver_standing2.select(col('full name').alias('Driver'),'Team','Wins','Points','year')
# bbc_driver_standing2.filter(col('year')==2024).orderBy(desc(col('Points'))).display()
bbc_driver_standing2.display()

# COMMAND ----------

# writing down the bbc driver_standings_df2 to gold

bbc_driver_standing2.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formula-one-project/gold/bbc_driver_standings')
