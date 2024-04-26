# Databricks notebook source
# MAGIC %run ../cmn_utils/cmn_functions

# COMMAND ----------

dbutils.secrets.list("f1_racing_secret_scope")

# COMMAND ----------

create_mnt_point("saf1racing", "bronze")

# COMMAND ----------

create_mnt_point("saf1racing", "silver")

# COMMAND ----------

create_mnt_point("saf1racing", "gold")

# COMMAND ----------

create_mnt_point("saf1racing", "formula-one-project")

# COMMAND ----------

# unmount_method('saf1racing','bronze')
# unmount_method('saf1racing','silver')
# unmount_method('saf1racing','gold')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


