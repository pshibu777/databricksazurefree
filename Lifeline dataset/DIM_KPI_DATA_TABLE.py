# Databricks notebook source
DIM_KPI_DATA_TABLE_BRONZE = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/lifeline rawdata/DIM_KPI_DATA_TABLE.csv")

# COMMAND ----------

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

Azure_DIM_DATE_DATA_TABLE_SILVER_DF = (spark.read
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

Azure_DIM_DATE_DATA_TABLE_SILVER_DF = Azure_DIM_DATE_DATA_TABLE_SILVER_DF.select(col("PK_KPI_ID"), 
col("KPI_GROUP"), 
col("KPI_NAME"), 
col("KPI_SEQUENCE"), 
col("SOURCE_IDENTIFIER_KEY"), 
col("DW_CREATED_DATE"), to_date(col("DW_MODIFIED_DATE"), "dd-mm-yy").alias("DW_MODIFIED_DATE"))

# COMMAND ----------

display(Azure_DIM_DATE_DATA_TABLE_SILVER_DF)

# COMMAND ----------

delta_table_path = "/mnt/to/delta/table/Azure_DIM_DATE_DATA_TABLE_SILVER_DF"

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_SILVER")
  .option("user", user)
  .option("password", password)
  .option("inferSchema", "true")
  .load())

from pyspark.sql.types import StringType, IntegerType, DateType

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN = Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.withColumn("PK_KPI_ID", col("PK_KPI_ID").cast(IntegerType())) \
    .withColumn("KPI_GROUP", col("KPI_GROUP").cast(StringType())) \
    .withColumn("KPI_NAME", col("KPI_NAME").cast(StringType())) \
    .withColumn("KPI_SEQUENCE", col("KPI_SEQUENCE").cast(IntegerType())) \
    .withColumn("SOURCE_IDENTIFIER_KEY", col("SOURCE_IDENTIFIER_KEY").cast(StringType())) \
    .withColumn("DW_CREATED_DATE", col("DW_CREATED_DATE").cast(DateType())) \
    .withColumn("DW_MODIFIED_DATE", col("DW_MODIFIED_DATE").cast(DateType())) \

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.write.format("delta").mode("overwrite").save(delta_table_path)

from delta.tables import DeltaTable
Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN = DeltaTable.forPath(spark, delta_table_path)

(Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.alias("DIM_KPI_DATA_TABLE_SILVER_MAIN").merge(Azure_DIM_DATE_DATA_TABLE_SILVER_DF.alias("DIM_KPI_DATA_TABLE_SILVER"), "DIM_KPI_DATA_TABLE_SILVER_MAIN.PK_KPI_ID = DIM_KPI_DATA_TABLE_SILVER.PK_KPI_ID") \
    .whenMatchedUpdate(set = 
                       {
                           "PK_KPI_ID" : "DIM_KPI_DATA_TABLE_SILVER.PK_KPI_ID",
                           "KPI_GROUP" : "DIM_KPI_DATA_TABLE_SILVER.KPI_GROUP",
                           "KPI_NAME" : "DIM_KPI_DATA_TABLE_SILVER.KPI_NAME",
                           "KPI_SEQUENCE" : "DIM_KPI_DATA_TABLE_SILVER.KPI_SEQUENCE",
                           "SOURCE_IDENTIFIER_KEY" : "DIM_KPI_DATA_TABLE_SILVER.SOURCE_IDENTIFIER_KEY",
                           "DW_CREATED_DATE" : "DIM_KPI_DATA_TABLE_SILVER.DW_CREATED_DATE",
                           "DW_MODIFIED_DATE" : "DIM_KPI_DATA_TABLE_SILVER.DW_MODIFIED_DATE"
                       }) \
    .whenNotMatchedInsert( values = 
                          {
                            "PK_KPI_ID" : "DIM_KPI_DATA_TABLE_SILVER.PK_KPI_ID",
                           "KPI_GROUP" : "DIM_KPI_DATA_TABLE_SILVER.KPI_GROUP",
                           "KPI_NAME" : "DIM_KPI_DATA_TABLE_SILVER.KPI_NAME",
                           "KPI_SEQUENCE" : "DIM_KPI_DATA_TABLE_SILVER.KPI_SEQUENCE",
                           "SOURCE_IDENTIFIER_KEY" : "DIM_KPI_DATA_TABLE_SILVER.SOURCE_IDENTIFIER_KEY",
                           "DW_CREATED_DATE" : "DIM_KPI_DATA_TABLE_SILVER.DW_CREATED_DATE",
                           "DW_MODIFIED_DATE" : "DIM_KPI_DATA_TABLE_SILVER.DW_MODIFIED_DATE"
                          }) \
    .execute()
    )

# COMMAND ----------

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN = Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.toDF()

Azure_DIM_DATE_DATA_TABLE_SILVER_MAIN.write \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("dbtable", "Lifeline.DIM_KPI_DATA_TABLE_SILVER") \
    .option("user", user) \
    .option("password", password) \
    .mode("overwrite") \
    .save()
