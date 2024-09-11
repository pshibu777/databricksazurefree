# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file into bronze dataframe

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/pshibu777@gmail.com/databricksazurefree/Lifeline dataset/Connections"

# COMMAND ----------

filelocation = f"abfss://lifeline@storagedatabrickshibu.dfs.core.windows.net/"+FACT_KPI_DATA_TABLE


# COMMAND ----------

FACT_KPI_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(filelocation)

display(FACT_KPI_DATA_TABLE_BRONZE.printSchema)

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "sqlserverdatabricks.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "SQLdatabaseazure"
user = "pshibu777"
password = "24Arun12!"
url = f"jdbc:sqlserver://sqlserverdatabricks.database.windows.net:1433;database=SQLdatabaseazure;user=pshibu777@sqlserverdatabricks;password=24Arun12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

FACT_KPI_DATA_TABLE_BRONZE.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_BRONZE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC read the bronze table from AZURE SQL database and transform the silver dataframe and load to AZURE SQL database.

# COMMAND ----------

FACT_KPI_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

FACT_KPI_DATA_TABLE_SILVER_DF = FACT_KPI_DATA_TABLE_SILVER_DF.withColumn("FK_KPI_ID", col("FK_KPI_ID").cast(IntegerType())) \
    .withColumn("FK_KPI_DATE_ID", col("FK_KPI_DATE_ID").cast(IntegerType())) \
    .withColumn("KPI_VALUE", col("KPI_VALUE").cast(IntegerType())) \
    .withColumn("SOURCE_INTEGRATION_ID", col("SOURCE_INTEGRATION_ID").cast(StringType())) \
    .withColumn("DW_CREATED_DATE", col("DW_CREATED_DATE").cast(DateType())) \
    .withColumn("DW_MODIFIED_DATE", col("DW_MODIFIED_DATE").cast(DateType()))

FACT_KPI_DATA_TABLE_SILVER_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Read the silver table from AZURE SQL database and push to gold table.

# COMMAND ----------

FACT_KPI_DATA_TABLE_GOLD_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

FACT_KPI_DATA_TABLE_GOLD_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
