# Databricks notebook source
# create table drivers

from pyspark.sql.functions import col,explode,replace
drivers_df = spark.read.option("multiline", "true").json('/mnt/saf1racing/formulaoneproject/bronze/drivers',recursiveFileLookup=True)
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

print(drivers_df.columns)
print(drivers_df.count())

# COMMAND ----------

drivers_df = drivers_df.select('driver_id', 'driver_ref', 'number', 'code', 'forename', 'surname', 'dob', 'nationality', 'url').distinct()
print(drivers_df.count())


# COMMAND ----------

# we retrieve all the columns required for our feature analysis
# we can directly write the drivers delta table in silver storage account

# COMMAND ----------

drivers_df.write.option('format','delta').mode('overwrite').save('/mnt/saf1racing/formulaoneproject/silver/drivers')

# COMMAND ----------


