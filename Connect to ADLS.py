# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("tarhone-fabric-adb-scope")

# COMMAND ----------

scope="tarhone-fabric-adb-scope"
key="fabric-demo-secret"
storage_account="databricksadlsgen2test"
application_id="0fb7d8bf-fbff-4ccd-bfbf-75c6828c7769"
directory_id="16b3c013-d300-468d-ac64-7eda0820b6d3"
service_credential = dbutils.secrets.get(scope=scope,key=key)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net"%(storage_account), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.%s.dfs.core.windows.net"%(storage_account), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net"%(storage_account), application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net"%(storage_account), service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net"%(storage_account), "https://login.microsoftonline.com/%s/oauth2/token"%(directory_id))

# COMMAND ----------

 container_name = "medallion"
 dbutils.fs.ls("abfss://%s@%s.dfs.core.windows.net/"%(container_name,storage_account))
