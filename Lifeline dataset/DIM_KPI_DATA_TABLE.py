# Databricks notebook source
# MAGIC %run "/Workspace/Repos/pshibu777@gmail.com/databricksazurefree/Lifeline dataset/Connections"

# COMMAND ----------

filelocation = f"abfss://lifeline@storagedatabrickshibu.dfs.core.windows.net/"+DIM_KPI_DATA_TABLE


# COMMAND ----------

# MAGIC %md
# MAGIC Get the csv file from AZURE storage account and save as bronze table in AZURE SQL database

# COMMAND ----------

DIM_KPI_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(filelocation)

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "sqlserverdatabricks.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "SQLdatabaseazure"
user = "pshibu777"
password = "24Arun12!"
url = f"jdbc:sqlserver://sqlserverdatabricks.database.windows.net:1433;database=SQLdatabaseazure;user=pshibu777@sqlserverdatabricks;password=24Arun12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

DIM_KPI_DATA_TABLE_BRONZE.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_BRONZE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC read the bronze table from AZURE SQL database and transform the silver dataframe and load to AZURE SQL database.

# COMMAND ----------

DIM_DATE_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

from pyspark.sql.functions import col, to_date

DIM_DATE_DATA_TABLE_SILVER_DF = DIM_DATE_DATA_TABLE_SILVER_DF.select(col("PK_KPI_ID"), 
col("KPI_GROUP"), 
col("KPI_NAME"), 
col("KPI_SEQUENCE"), 
col("SOURCE_IDENTIFIER_KEY"), 
col("DW_CREATED_DATE"), to_date(col("DW_MODIFIED_DATE"), "dd-mm-yy").alias("DW_MODIFIED_DATE"))

DIM_DATE_DATA_TABLE_SILVER_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Read the silver table from AZURE SQL database and push to gold table.

# COMMAND ----------

DIM_DATE_DATA_TABLE_GOLD_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

DIM_DATE_DATA_TABLE_GOLD_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
