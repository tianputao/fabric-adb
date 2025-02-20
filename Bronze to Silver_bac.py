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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Temp Views, clean the data, load into Spark Dataframes
# MAGIC - Fileter Rows
# MAGIC - Rename Columns
# MAGIC - Drop Columns
# MAGIC

# COMMAND ----------

df_salesltsalesorderdetail = spark.sql("SELECT SalesOrderID, OrderQty, ProductID,UnitPrice, UnitPriceDIscount, LineTotal FROM salesltsalesorderdetail")
display(df_salesltsalesorderdetail)

# COMMAND ----------

df_salesltsalesorderheader = spark.sql("SELECT SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, ShipToAddressID, BillToAddressID, ShipMethod, SubTotal, TaxAmt, Freight, TotalDue FROM salesltsalesorderheader")
display(df_salesltsalesorderheader)

# COMMAND ----------

# Randomizing the dates in the OrderDate column since our toy AdventureWorks LT dataset only has one distinct order date.
from pyspark.sql.functions import rand, col, expr

df_salesltsalesorderheader = df_salesltsalesorderheader.drop("OrderDate").withColumn("OrderDate", expr("date_add(current_date()-1000, CAST(rand() * 365 AS INT))"))
display(df_salesltsalesorderheader)

# COMMAND ----------

df_salesltcustomer = spark.sql("SELECT CustomerID, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, EmailAddress, Phone  FROM salesltcustomer")
display(df_salesltcustomer)

# COMMAND ----------

df_salesltcustomeraddress = spark.sql("SELECT CustomerID, AddressID, AddressType FROM salesltcustomeraddress")
display(df_salesltcustomeraddress)

# COMMAND ----------

df_salesltproduct = spark.sql("SELECT ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductCategoryID FROM salesltproduct")
display(df_salesltproduct)

# COMMAND ----------

df_salesltproductcategory = spark.sql("SELECT ProductCategoryID, ParentProductCategoryID, Name FROM salesltproductcategory")
display(df_salesltproductcategory)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Data Frames into ADLS Silver layer as External Tables
# MAGIC

# COMMAND ----------

path="abfss://%s@%s.dfs.core.windows.net/silver"%(container_name,storage_account)
tableName="salesOrderHeader"
dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_salesltsalesorderheader.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

tableName="salesOrderDetail"

dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_salesltsalesorderdetail.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

tableName="salesCustomer"

dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_salesltcustomer.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

tableName="salesProduct"

dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_salesltproduct.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")

# COMMAND ----------

tableName="salesProductCategory"

dbutils.fs.mkdirs("%s/%s"%(path,tableName))
df_salesltproductcategory.write.saveAsTable(tableName, format="delta", mode="overwrite", overwriteSchema = "true", path=path + "/" + tableName + "/")
