# Databricks notebook source
# create table drivers

from pyspark.sql.functions import col,explode,replace
drivers_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/drivers/2024-04-14')
drivers_df = drivers_df.withColumn('list',explode(col('MRData').DriverTable.Drivers))
drivers_df = drivers_df.drop('MRData')
drivers_df = drivers_df.withColumn('driver_id',col('list').driverId).withColumn('driver_ref',col('list').familyName)\
    .withColumn('number',col('list').PermanentNumber).withColumn('code',col('list').code).withColumn('forename',col('list').givenName).withColumn('surname',col('list').familyName).withColumn('dob',col('list').dateOfBirth).withColumn('nationality',col('list').nationality).withColumn('url',col('list').url)

drivers_df = drivers_df.drop('list')

drivers_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace,lower
drivers_df = drivers_df.withColumn('driver_ref',lower(regexp_replace(col('driver_ref'),' ','_')))
drivers_df = drivers_df.withColumn('driver_ref',regexp_replace(col('driver_ref'),'[^a-z]',''))
drivers_df.display()

# COMMAND ----------


