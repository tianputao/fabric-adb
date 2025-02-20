# Databricks notebook source
scope="tarhone-fabric-adb-scope"
key="fabric-demo-secret"
storage_account="databricksadlsgen2test"
application_id="0fb7d8bf-fbff-4ccd-bfbf-75c6828c7769"
directory_id= "16b3c013-d300-468d-ac64-7eda0820b6d3"
service_credential = dbutils.secrets.get(scope=scope,key=key)
spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net"%(storage_account), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.%s.dfs.core.windows.net"%(storage_account), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net"%(storage_account), application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net"%(storage_account), service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net"%(storage_account), "https://login.microsoftonline.com/%s/oauth2/token"%(directory_id))

# COMMAND ----------

container_name = "medallion"
# Get all the files under the ADLS folder and create a list of file paths
file_list = dbutils.fs.ls("abfss://%s@%s.dfs.core.windows.net/silver"%(container_name,storage_account))
# Read each file and create a DataFrame
for file_path in file_list:
    print(file_path)
    df = spark.read.format("delta").load(path=file_path.path[:-1])
    # You can process the DataFrame or register it as a table here
    # For example, to create a temporary table:
    df.createOrReplaceTempView(file_path.name[:-1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

#Create DimCustomer
df_dimCustomer = spark.sql("select sc.* , sca.AddressID, sca.AddressType from salescustomer sc join salescustomeraddress sca on sc.customerid = sca.customerid")

#Add surrogate key as the first column
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn("CustomerIDKey", monotonically_increasing_id())\
    .select(
        "CustomerIDKey",  # Select the surrogate key column first
        *[column for column in df_dimCustomer.columns if column != "CustomerIDKey"]  # Select the remaining columns in their original order
    )
