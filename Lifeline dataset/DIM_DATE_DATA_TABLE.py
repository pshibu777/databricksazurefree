# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file into bronze dataframe

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/pshibu777@gmail.com/databricksazurefree/Lifeline dataset/Connections"

# COMMAND ----------

filelocation = f"abfss://lifeline@storagedatabrickshibu.dfs.core.windows.net/"+DIM_DATE_DATA_TABLE


# COMMAND ----------

DIM_DATE_DATA_TABLE_BRONZE_DF = spark.read.format("csv") \
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

DIM_DATE_DATA_TABLE_BRONZE_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_BRONZE") \
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
  .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

DIM_DATE_DATA_TABLE_SILVER_DF = DIM_DATE_DATA_TABLE_SILVER_DF.withColumn("CAL_QUARTER", col("CAL_QUARTER").cast(IntegerType())) \
    .withColumn("CAL_YEAR", col("CAL_YEAR").cast(IntegerType())) \
    .withColumn("DAY", col("DAY").cast(IntegerType())) \
    .withColumn("DAY_OF_YEAR", col("DAY_OF_YEAR").cast(IntegerType())) \
    .withColumn("FISCAL_MONTH", col("FISCAL_MONTH").cast(IntegerType())) \
    .withColumn("FISCAL_QUARTER", col("FISCAL_QUARTER").cast(IntegerType())) \
    .withColumn("FISCAL_WEEK", col("FISCAL_WEEK").cast(IntegerType())) \
    .withColumn("FISCAL_YEAR", col("FISCAL_YEAR").cast(IntegerType())) \
    .withColumn("PK_DATE_ID", col("PK_DATE_ID").cast(IntegerType())) \
    .withColumn("WEEK_OF_MONTH", col("WEEK_OF_MONTH").cast(IntegerType())) \
    .withColumn("WEEK_OF_YEAR", col("WEEK_OF_YEAR").cast(IntegerType())) \
    .withColumn("YEARMONTHNO", col("YEARMONTHNO").cast(IntegerType()))

DIM_DATE_DATA_TABLE_SILVER_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_SILVER") \
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
  .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

DIM_DATE_DATA_TABLE_GOLD_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
