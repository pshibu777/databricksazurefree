# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file into bronze dataframe

# COMMAND ----------

FACT_SERVICE_PROVIDER_PERF_DATA_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/lifeline rawdata/FACT_SERVICE_PROVIDER_PERF_DATA_TABLE.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC JDBS connection string and wirte to bronze

# COMMAND ----------

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
# MAGIC read the silver table from azure SQL database, transform and create new primary key

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.select('*', concat(col('FK_SERVICE_PROVIDER_ID'), lit('~'), col('ORDER_SEQ')).alias("CONCATED_ID"))

display(FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, StringType

delta_table_path = "/mnt/to/delta/table/FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER"

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.withColumn("FK_SERVICE_PROVIDER_ID", col('FK_SERVICE_PROVIDER_ID').cast(IntegerType())) \
    .withColumn("FK_DATE_ID", col("FK_DATE_ID").cast(IntegerType())) \
    .withColumn("CURR_VALUE", col("CURR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("PRIOR_VALUE", col("PRIOR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("THRESHOLD_VALUE", col("THRESHOLD_VALUE").cast(IntegerType())) \
    .withColumn("ORDER_SEQ", col("ORDER_SEQ").cast(IntegerType())) \
    .withColumn("CONCATED_ID", col("CONCATED_ID").cast(StringType()))

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_MAIN.write.format("delta").mode("overwrite").save(delta_table_path)

# COMMAND ----------

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

from pyspark.sql.types import TimestampType, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, concat, lit

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE.withColumn("FK_SERVICE_PROVIDER_ID", col("FK_SERVICE_PROVIDER_ID").cast(IntegerType())) \
    .withColumn("FK_DATE_ID", col("FK_DATE_ID").cast(IntegerType())) \
    .withColumn("ATTRIBUTE_GROUP", col("ATTRIBUTE_GROUP").cast(StringType())) \
    .withColumn("CURR_VALUE", col("CURR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("PRIOR_VALUE", col("PRIOR_VALUE").cast(DecimalType(22, 2))) \
    .withColumn("THRESHOLD_VALUE", col("THRESHOLD_VALUE").cast(IntegerType())) \
    .withColumn("ORDER_SEQ", col("ORDER_SEQ").cast(IntegerType())) \
    .select ('*', concat(col("FK_SERVICE_PROVIDER_ID"), lit("~"), col("ORDER_SEQ")).alias("CONCATED_ID"))


# COMMAND ----------

# MAGIC %md
# MAGIC merge the with bronze dataframe to silver Deltatable and save the table in Azure SQL database

# COMMAND ----------

from delta.tables import DeltaTable

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER = DeltaTable.forPath(spark, delta_table_path)

(FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.alias("FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_MAIN") \
    .merge(FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_BRONZE.alias("FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER"), "FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER_MAIN.CONCATED_ID = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.CONCATED_ID") \
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )

# COMMAND ----------

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER = FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.toDF()

FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.FACT_SERVICE_PROVIDER_PERF_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
