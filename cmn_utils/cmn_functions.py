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


