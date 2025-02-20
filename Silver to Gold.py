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

# COMMAND ----------

display(df_dimCustomer_with_surrogate_key)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

#Create dimProduct
df_dimProduct = spark.sql("select sc.* , sca.AddressID, sca.AddressType from salescustomer sc join salescustomeraddress sca on sc.customerid = sca.customerid")
#Add surrogate key as the first column
df_dimProduct_with_surrogate_key = df_dimProduct.withColumn("ProductIDKey", monotonically_increasing_id())\
    .select(
        "ProductIDKey",  # Select the surrogate key column first
        *[column for column in df_dimProduct.columns if column != "ProductIDKey" and column!="spc.Name"]  # Select the remaining columns in their original order
    )

# COMMAND ----------

display(df_dimProduct_with_surrogate_key)

# COMMAND ----------

from pyspark.sql.functions import expr
 
# Define the start and end dates for your DimDate table
start_date = "2000-01-01"
end_date = "2024-12-31"
 
# Create a DataFrame with a range of dates
df_dimDate = spark.range(0, (spark.sql("SELECT datediff('{0}', '{1}')".format(end_date, start_date)).collect()[0][0])+1) \
    .selectExpr("CAST(id AS INT) AS id") \
    .selectExpr("date_add('{0}', id) AS Date".format(start_date))
 
# Extract different date components
df_dimDate = df_dimDate \
    .withColumn("Year", expr("year(Date)")) \
    .withColumn("Month", expr("month(Date)")) \
    .withColumn("DayOfMonth", expr("dayofmonth(Date)")) \
    .withColumn("DayOfYear", expr("dayofyear(Date)")) \
    .withColumn("WeekOfYear", expr("weekofyear(Date)")) \
    .withColumn("DayOfWeek", expr("dayofweek(Date)")) \
    .withColumn("Quarter", expr("quarter(Date)"))

display(df_dimDate)

# COMMAND ----------

df1 = spark.sql("select * from salesorderheader limit 2")
df2 = spark.sql("select * from salesorderdetail limit 2")

display(df1)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write dataframes to Gold Layer as External Tables

# COMMAND ----------

# Write dimProduct table into ADLS Gold layer as External Tables

path="abfss://%s@%s.dfs.core.windows.net/gold"%(container_name,storage_account)

tableName="dimProduct"
dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_dimProduct_with_surrogate_key.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

# Write dimCustomer table into ADLS Gold layer as External Tables

path="abfss://%s@%s.dfs.core.windows.net/gold"%(container_name,storage_account)

tableName="dimCustomer"
dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_dimCustomer_with_surrogate_key.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

#Create factSales
df_factSales = spark.sql("select dp.ProductIDKey, ds.CustomerIDKey, soh.*, sod.OrderQty, sod.ProductID, sod.UnitPrice, sod.UnitPriceDiscount, sod.LineTotal from salesorderheader soh join salesorderdetail sod on soh.SalesOrderID = sod.SalesOrderID LEFT JOIN dimProduct dp ON sod.ProductID = dp.ProductIDKey LEFT JOIN dimCustomer ds ON soh.CustomerID = ds.CustomerID" )
display(df_factSales)


# COMMAND ----------

# Write dimDate table into ADLS Gold layer as External Tables

path="abfss://%s@%s.dfs.core.windows.net/gold"%(container_name,storage_account)

tableName="dimDate"
dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_dimDate.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

# Write factSales table into ADLS Gold layer as External Tables

path="abfss://%s@%s.dfs.core.windows.net/gold"%(container_name,storage_account)

tableName="factSales"
dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_factSales.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------


