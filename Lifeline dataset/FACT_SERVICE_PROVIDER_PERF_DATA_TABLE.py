# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file into bronze dataframe

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/pshibu777@gmail.com/databricksazurefree/Lifeline dataset/Connections"

# COMMAND ----------

filelocation = f"abfss://lifeline@storagedatabrickshibu.dfs.core.windows.net/"+FACT_SERVICE_PROVIDER_PERF_DATA_TABLE


# COMMAND ----------

FACT_SERVICE_PROVIDER_PERF_DATA_BRONZE = spark.read.format("csv") \
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

FACT_SERVICE_PROVIDER_PERF_DATA_BRONZE.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC read the bronze table from AZURE SQL database, transform and create new primary keythe silver dataframe and load to AZURE SQL database.

# COMMAND ----------

from pyspark.sql.types import TimestampType, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, concat, lit

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_DF = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_DF.withColumn("FK_SERVICE_PROVIDER_ID", col('FK_SERVICE_PROVIDER_ID').cast(IntegerType())) \
    .withColumn("FK_DATE_ID", col("FK_DATE_ID").cast(IntegerType())) \
    .withColumn("CURR_VALUE", col("CURR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("PRIOR_VALUE", col("PRIOR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("THRESHOLD_VALUE", col("THRESHOLD_VALUE").cast(IntegerType())) \
    .withColumn("ORDER_SEQ", col("ORDER_SEQ").cast(IntegerType()))

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save() 

# COMMAND ----------

# MAGIC %md
# MAGIC Read the silver table from AZURE SQL database and push to gold table.

# COMMAND ----------

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_GOLD_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_GOLD_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
