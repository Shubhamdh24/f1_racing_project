# Databricks notebook source


# COMMAND ----------

dbutils.secrets.list('f1_racing_secret_scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='f1_racing_secret_scope', key='saf1racing-client-id')
tenant_id = dbutils.secrets.get(scope = 'f1_racing_secret_scope', key='saf1racing-tenant-id')
client_secret = dbutils.secrets.get(scope = 'f1_racing_secret_scope', key='saf1racing-client-secret')

# COMMAND ----------

config_for_service_principal = {'fs.azure.account.auth.type' :'OAuth',
'fs.azure.account.oauth.provider.type':'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
'fs.azure.account.oauth2.client.id':client_id,
'fs.azure.account.oauth2.client.secret':client_secret,
'fs.azure.account.oauth2.client.endpoint':f'https://login.microsoftonline.com/{tenant_id}/oauth2/token',
'fs.azure.createRemoteFileSystemDuringInitialization': 'true'}

# COMMAND ----------


def create_mnt_point(storage_acc_name,container_name):

    config_for_service_principal = {'fs.azure.account.auth.type' :'OAuth',
    'fs.azure.account.oauth.provider.type':'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
    'fs.azure.account.oauth2.client.id':client_id,
    'fs.azure.account.oauth2.client.secret':client_secret,
    'fs.azure.account.oauth2.client.endpoint':f'https://login.microsoftonline.com/{tenant_id}/oauth2/token',
    'fs.azure.createRemoteFileSystemDuringInitialization': 'true'}

    if
    

    dbutils.fs.mount(source=f'abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net',
                 mount_point=f'/mnt/{storage_acc_name}/{container_name}',
                 extra_configs= config_for_service_principal)


def unmount_method(storage_acc_name,container_name):
    dbutils.fs.unmount(f'/mnt/{storage_acc_name}/{container_name}')


# COMMAND ----------


create_mnt_point('saf1racing','bronze')
create_mnt_point('saf1racing','silver')
create_mnt_point('saf1racing','gold')


# COMMAND ----------

unmount_method('saf1racing','bronze')
unmount_method('saf1racing','silver')
unmount_method('saf1racing','gold')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


