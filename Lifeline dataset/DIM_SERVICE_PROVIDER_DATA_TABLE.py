# Databricks notebook source
# MAGIC %run "/Workspace/Repos/pshibu777@gmail.com/databricksazurefree/Lifeline dataset/Connections"

# COMMAND ----------

filelocation = f"abfss://lifeline@storagedatabrickshibu.dfs.core.windows.net/"+DIM_SERVICE_PROVIDER_DATA_TABLE


# COMMAND ----------

DIM_SERVICE_PROVIDER_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(filelocation)

display(DIM_SERVICE_PROVIDER_DATA_TABLE_BRONZE.printSchema)

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "sqlserverdatabricks.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "SQLdatabaseazure"
user = "pshibu777"
password = "24Arun12!"
url = f"jdbc:sqlserver://sqlserverdatabricks.database.windows.net:1433;database=SQLdatabaseazure;user=pshibu777@sqlserverdatabricks;password=24Arun12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

DIM_SERVICE_PROVIDER_DATA_TABLE_BRONZE.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_SERVICE_PROVIDER_DATA_TABLE_BRONZE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

DIM_KPI_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(filelocation)

# COMMAND ----------

# MAGIC %md
# MAGIC read the bronze table from AZURE SQL database and transform the silver dataframe and load to AZURE SQL database.

# COMMAND ----------

DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_SERVICE_PROVIDER_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

display(DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER_DF.printSchema)

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.functions import col

DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER_DF = DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER_DF.withColumn("PK_SERVICE_PROVIDER_ID", col("PK_SERVICE_PROVIDER_ID").cast(IntegerType())) \
    .withColumn("SERVICE_PROVIDER_TYPE", col("SERVICE_PROVIDER_TYPE").cast(StringType())) \
    .withColumn("SERVICE_PROVIDER_NAME", col("SERVICE_PROVIDER_NAME").cast(StringType())) \
    .withColumn("SERVICE_PROVIDER_RANK", col("SERVICE_PROVIDER_RANK").cast(IntegerType())) \
    .withColumn("SERVICE_PROVIDER_SPECIALITY", col("SERVICE_PROVIDER_SPECIALITY").cast(StringType())) \
    .withColumn("MCRDCRNO", col("MCRDCRNO").cast(IntegerType())) \
    .withColumn("DOCTOR_TYPE", col("DOCTOR_TYPE").cast(StringType())) \
    .withColumn("REMARKS", col("REMARKS").cast(StringType())) \
    .withColumn("SOURCE_KEY", col("SOURCE_KEY").cast(StringType())) \
    .withColumn("SOURCE_IDENTIFIER_KEY", col("SOURCE_IDENTIFIER_KEY").cast(StringType())) \
    .withColumn("RECORD_CREATED_DATE", col("RECORD_CREATED_DATE").cast(DateType())) \
    .withColumn("RECORD_CREATED_BY", col("RECORD_CREATED_BY").cast(IntegerType())) \
    .withColumn("RECORD_MODIFIED_DATE", col("RECORD_MODIFIED_DATE").cast(DateType())) \
    .withColumn("RECORD_MODIFIED_BY", col("RECORD_MODIFIED_BY").cast(IntegerType())) \
    .withColumn("RECORD_CANCELLED_DATE", col("RECORD_CANCELLED_DATE").cast(DateType())) \
    .withColumn("RECORD_CANCELLED_BY", col("RECORD_CANCELLED_DATE").cast(IntegerType())) \
    .withColumn("SERVICE_PROVIDER_CODE", col("SERVICE_PROVIDER_CODE").cast(StringType())) \
    .withColumn("EFFECTIVE_START_DATE", col("EFFECTIVE_START_DATE").cast(DateType())) \
    .withColumn("EFFECTIVE_END_DATE", col("EFFECTIVE_END_DATE").cast(DateType())) \
    .withColumn("DW_CREATED_DATE", col("DW_CREATED_DATE").cast(DateType())) \
    .withColumn("DW_MODIFIED_DATE", col("DW_MODIFIED_DATE").cast(DateType())) \
    .withColumn("SERVICE_PROVIDER_RANK_CODE", col("SERVICE_PROVIDER_RANK_CODE").cast(StringType()))

# COMMAND ----------

DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Read the silver table from AZURE SQL database and push to gold table.

# COMMAND ----------

DIM_SERVICE_PROVIDER_DATA_TABLE_GOLD_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_SERVICE_PROVIDER_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

DIM_SERVICE_PROVIDER_DATA_TABLE_GOLD_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_SERVICE_PROVIDER_DATA_TABLE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
