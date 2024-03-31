# Databricks notebook source
def create_mnt_point(storage_acc_name, container_name):

    client_id = dbutils.secrets.get(scope="f1_racing_secret_scope", key="saf1racing-client-id")
    tenant_id = dbutils.secrets.get(scope="f1_racing_secret_scope", key="saf1racing-tenant-id")
    client_secret = dbutils.secrets.get(scope="f1_racing_secret_scope", key="saf1racing-client-secret")

    config_for_service_principal = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        "fs.azure.createRemoteFileSystemDuringInitialization": "true"
    }

    list_of_mount_points = []
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}":
            list_of_mount_points.append(mount.mountPoint)
        else:
            pass

    if any(list_of_mount_points):
        dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net",
            mount_point=f"/mnt/{storage_acc_name}/{container_name}",
            extra_configs=config_for_service_principal
        )

    else:
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net",
            mount_point=f"/mnt/{storage_acc_name}/{container_name}",
            extra_configs=config_for_service_principal
        )

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,\
    IntegerType,FloatType,DateType
from datetime import datetime,date
from pyspark.sql.functions import col,lit,current_timestamp,current_date,concat_ws,concat

# COMMAND ----------

def add_ingestion_date(data_frame):
    data_frame = data_frame.withColumn('ingestion_date',current_date())
    return data_frame

# COMMAND ----------

def write_file(data_frame,container_name,file_name):
    data_frame.write.mode('overwrite').parquet(f'/mnt/saf1racing/{container_name}/{file_name}.parquet')
    return data_frame
