# Databricks notebook source
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
# Get all the files under the ADLS folder and create a list of file paths
file_list = dbutils.fs.ls("abfss://%s@%s.dfs.core.windows.net/bronze"%(container_name,storage_account))
for file_info in file_list:
    print(file_info)
    df = spark.read.format("csv").options(inferSchema="true", header="true").load(path=f"{file_info.path}*")
    
    # Sanitize the view name by replacing periods with underscores
    view_name = file_info.name.removesuffix('.csv').replace('.', '')
    
    # Create a temporary table
    df.createOrReplaceTempView(view_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM salesltaddress LIMIT 100
