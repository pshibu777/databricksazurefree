# Databricks notebook source
FACT_HR_PERF_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/lifeline rawdata/FACT_HR_PERF_DATA_TABLE.csv")

display(FACT_HR_PERF_DATA_TABLE_BRONZE.printSchema)

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "sqlserverdatabricks.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "SQLdatabaseazure"
user = "pshibu777"
password = "24Arun12!"
url = f"jdbc:sqlserver://sqlserverdatabricks.database.windows.net:1433;database=SQLdatabaseazure;user=pshibu777@sqlserverdatabricks;password=24Arun12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

FACT_HR_PERF_DATA_TABLE_BRONZE.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_HR_PERF_DATA_TABLE_BRONZE") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

FACT_HR_PERF_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_HR_PERF_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

# COMMAND ----------

FACT_HR_PERF_DATA_TABLE_SILVER = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_HR_PERF_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

# COMMAND ----------

delta_table_path = "/mnt/to/delta/table/FACT_HR_PERF_DATA_TABLE_SILVER"

from pyspark.sql.types import StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import col

FACT_HR_PERF_DATA_TABLE_SILVER = FACT_HR_PERF_DATA_TABLE_SILVER.withColumn("FK_DATE_ID", col("FK_DATE_ID").cast(IntegerType())) \
    .withColumn("ATTRIBUTE_GROUP", col("ATTRIBUTE_GROUP").cast(StringType())) \
    .withColumn("ATTRIBUTE", col("ATTRIBUTE").cast(StringType())) \
    .withColumn("CURR_QTR", col("CURR_QTR").cast(DecimalType(22,7))) \
    .withColumn("YTD", col("YTD").cast(DecimalType(22,7))) \
    .withColumn("THRESHOLD_VALUE", col("THRESHOLD_VALUE").cast(IntegerType())) \
    .withColumn("ORDER_SEQ", col("ORDER_SEQ").cast(IntegerType()))

# COMMAND ----------

FACT_HR_PERF_DATA_TABLE_SILVER.write.format("delta").mode("overwrite").save(delta_table_path)

from delta.tables import DeltaTable
FACT_HR_PERF_DATA_TABLE_SILVER = DeltaTable.forPath(spark, delta_table_path)


(FACT_HR_PERF_DATA_TABLE_SILVER.alias("FACT_HR_PERF_DATA_TABLE_SILVER_MAIN").merge(
    FACT_HR_PERF_DATA_TABLE_SILVER_DF.alias("FACT_HR_PERF_DATA_TABLE_SILVER"), "FACT_HR_PERF_DATA_TABLE_SILVER_MAIN.ORDER_SEQ = FACT_HR_PERF_DATA_TABLE_SILVER.ORDER_SEQ") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        )

# COMMAND ----------

FACT_HR_PERF_DATA_TABLE_SILVER = FACT_HR_PERF_DATA_TABLE_SILVER.toDF()

FACT_HR_PERF_DATA_TABLE_SILVER.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_HR_PERF_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
