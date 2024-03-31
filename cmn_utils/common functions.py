# Databricks notebook source
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


