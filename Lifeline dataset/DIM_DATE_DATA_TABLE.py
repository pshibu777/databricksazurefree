# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file into bronze dataframe

# COMMAND ----------

DIM_DATE_DATA_TABLE_BRONZE_DF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/lifeline rawdata/DIM_DATE_DATA_TABLE.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC JDBS connection string

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "sqlserverdatabricks.database.windows.net"
database_port = "1433" # update if you use a non-default port
database_name = "SQLdatabaseazure"
table = "Lifeline.DIM_DATE_DATA_TABLE_BRONZE"
user = "pshibu777"
password = "24Arun12!"
url = f"jdbc:sqlserver://sqlserverdatabricks.database.windows.net:1433;database=SQLdatabaseazure;user=pshibu777@sqlserverdatabricks;password=24Arun12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# MAGIC %md
# MAGIC Write to the bronze table
# MAGIC

# COMMAND ----------

DIM_DATE_DATA_TABLE_BRONZE_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC read the bronze table from azure SQL database

# COMMAND ----------

Azure_DIM_DATE_DATA_TABLE_BRONZE = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", table)
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load()
)

# COMMAND ----------

display(Azure_DIM_DATE_DATA_TABLE_BRONZE)

# COMMAND ----------

# MAGIC %md
# MAGIC read the silver table from azure SQL database

# COMMAND ----------

Azure_DIM_DATE_DATA_TABLE_SILVER = (spark.read
  .format("jdbc")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC transform the silver dataframe and load to Delta

# COMMAND ----------

delta_table_path = "/mnt/to/delta/table/Azure_DIM_DATE_DATA_TABLE_SILVER"

from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.functions import col

Azure_DIM_DATE_DATA_TABLE_SILVER = Azure_DIM_DATE_DATA_TABLE_SILVER.withColumn("CAL_DATE", col("CAL_DATE").cast(StringType()))
Azure_DIM_DATE_DATA_TABLE_SILVER = Azure_DIM_DATE_DATA_TABLE_SILVER.withColumn("CAL_DATE", col("CAL_DATE").cast(TimestampType())) \
    .withColumn("CAL_QUARTER_NAME", col("CAL_QUARTER_NAME").cast(StringType())) \
    .withColumn("DAY_NAME", col("DAY_NAME").cast(StringType())) \
    .withColumn("FISCAL_QUARTER_NAME", col("FISCAL_QUARTER_NAME").cast(StringType())) \
    .withColumn("MONTH", col("MONTH").cast(StringType())) \
    .withColumn("MONTH_NAME", col("MONTH_NAME").cast(StringType())) \
    .withColumn("PH_DESC", col("PH_DESC").cast(StringType())) \
    .withColumn("QUARTER", col("QUARTER").cast(StringType())) \
    .withColumn("WEEK_OF_YEAR_NAME", col("WEEK_OF_YEAR_NAME").cast(StringType()))

Azure_DIM_DATE_DATA_TABLE_SILVER.write.format("delta").mode("overwrite").save(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC merge the with bronze dataframe to silver Deltatable

# COMMAND ----------

from delta.tables import DeltaTable

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN = DeltaTable.forPath(spark, delta_table_path)
(Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.alias("DIM_DATE_DATA_TABLE_SILVER_MAIN") \
  .merge(Azure_DIM_DATE_DATA_TABLE_BRONZE.alias("DIM_DATE_DATA_TABLE_SILVER_DF"), "DIM_DATE_DATA_TABLE_SILVER_MAIN.PK_DATE_ID = DIM_DATE_DATA_TABLE_SILVER_DF.PK_DATE_ID") \
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )

# COMMAND ----------

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN_DF = Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.toDF()
Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN_DF.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_DATE_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
