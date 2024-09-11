# Databricks notebook source
FACT_KPI_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/lifeline rawdata/FACT_KPI_DATA_TABLE.csv")

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

FACT_KPI_DATA_TABLE_SILVER_DF = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

# COMMAND ----------

FACT_KPI_DATA_TABLE_SILVER = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

# COMMAND ----------

delta_table_path = "/mnt/to/delta/table/FACT_KPI_DATA_TABLE_SILVER"

from pyspark.sql.types import StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import col

FACT_KPI_DATA_TABLE_SILVER = FACT_KPI_DATA_TABLE_SILVER.withColumn("FK_KPI_ID", col("FK_KPI_ID").cast(IntegerType())) \
    .withColumn("FK_KPI_DATE_ID", col("FK_KPI_DATE_ID").cast(IntegerType())) \
    .withColumn("KPI_VALUE", col("KPI_VALUE").cast(IntegerType())) \
    .withColumn("SOURCE_INTEGRATION_ID", col("SOURCE_INTEGRATION_ID").cast(StringType())) \
    .withColumn("DW_CREATED_DATE", col("DW_CREATED_DATE").cast(DateType())) \
    .withColumn("DW_MODIFIED_DATE", col("DW_MODIFIED_DATE").cast(DateType()))

# COMMAND ----------


FACT_KPI_DATA_TABLE_SILVER.write.format("delta").mode("overwrite").save(delta_table_path)

from delta.tables import DeltaTable

FACT_KPI_DATA_TABLE_SILVER = DeltaTable.forPath(spark, delta_table_path)

(FACT_KPI_DATA_TABLE_SILVER.alias("FACT_KPI_DATA_TABLE_SILVER_TABLE").merge(FACT_KPI_DATA_TABLE_SILVER_DF.alias("FACT_KPI_DATA_TABLE_SILVER_DF"), "FACT_KPI_DATA_TABLE_SILVER_TABLE.SOURCE_INTEGRATION_ID = FACT_KPI_DATA_TABLE_SILVER_DF.SOURCE_INTEGRATION_ID") \
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
 )

# COMMAND ----------

FACT_KPI_DATA_TABLE_SILVER = FACT_KPI_DATA_TABLE_SILVER.toDF()

FACT_KPI_DATA_TABLE_SILVER.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_KPI_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
