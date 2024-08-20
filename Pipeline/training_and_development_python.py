# Databricks notebook source
service_credential = dbutils.secrets.get(scope="azurestoragescope", key="serviceprincipleAK")

spark.conf.set("fs.azure.account.auth.type.storagedatabrickshibu.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storagedatabrickshibu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storagedatabrickshibu.dfs.core.windows.net", "4a0002e3-fd7f-4075-90cc-0db75c40015d")
spark.conf.set("fs.azure.account.oauth2.client.secret.storagedatabrickshibu.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagedatabrickshibu.dfs.core.windows.net", "https://login.microsoftonline.com/c61f150e-93fa-41be-b0f6-b6f8b0c1220c/oauth2/token")

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

@dlt.table
def EMPLOYEE_TRAINING_RAW_DF_BRONZE():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .load("abfss://traininganddevelopment@storagedatabrickshibu.dfs.core.windows.net/")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            F.col("Employee ID").alias("Employee_ID"),
            F.col("Training Date").alias("Training_Date"),
            F.col("Training Program Name").alias("Training_Program_Name"),
            F.col("Training Type").alias("Training_Type"),
            F.col("Training Outcome").alias("Training_Outcome"),
            F.col("Location"),
            F.col("Trainer"),
            F.col("Training Duration(Days)").alias("Training_Duration_days"),
            F.col("Training Cost").alias("Training_Cost")
        )
    )

# COMMAND ----------

@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_employee_id", "Employee_id IS NOT NULL")
def EMPLOYEE_TRAINING_DF_SILVER():
    return (
        dlt.readStream("EMPLOYEE_TRAINING_RAW_DF_BRONZE")
        .select(
            F.col("Employee_ID").cast("int"),
            F.to_date(F.col("Training_Date"), "dd-MM-yyyy").alias("Training_date"),
            F.col("Training_Program_Name"),
            F.col("Training_Type"),
            F.col("Training_Outcome"),
            F.col("Location"),
            F.col("Trainer"),
            F.col("Training_Duration_days"),
            F.col("Training_Cost").cast("decimal(22,7)"),
            F.col("processing_time"),
            F.col("source_file")
        )
        )

# COMMAND ----------

@dlt.table(table_properties={"quality": "gold"})
def EMPLOYEE_TRAINING_DF_GOLD():
    return (
        dlt.readStream("EMPLOYEE_TRAINING_DF_SILVER")
    )
