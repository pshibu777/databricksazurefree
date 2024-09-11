# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

storage_account = dbutils.secrets.get(scope= "Secret-Scope" , key= "storaccname")
storage_account_key = dbutils.secrets.get(scope= "Secret-Scope" , key= "storacckey")

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
  storage_account_key)
 

# COMMAND ----------

DIM_DATE_DATA_TABLE = "DIM_DATE_DATA_TABLE.csv"
DIM_KPI_DATA_TABLE = "DIM_KPI_DATA_TABLE.csv"
DIM_SERVICE_PROVIDER_DATA_TABLE = "DIM_SERVICE_PROVIDER_DATA_TABLE.csv"
FACT_HR_PERF_DATA_TABLE = "FACT_HR_PERF_DATA_TABLE.csv"
FACT_KPI_DATA_TABLE = "FACT_KPI_DATA_TABLE.csv"
FACT_SERVICE_PROVIDER_PERF_DATA_TABLE = "FACT_SERVICE_PROVIDER_PERF_DATA_TABLE.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC
