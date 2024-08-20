# Databricks notebook source
service_credential = dbutils.secrets.get(scope="azurestoragescope", key="serviceprincipleAK")

spark.conf.set("fs.azure.account.auth.type.storagedatabrickshibu.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storagedatabrickshibu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storagedatabrickshibu.dfs.core.windows.net", "4a0002e3-fd7f-4075-90cc-0db75c40015d")
spark.conf.set("fs.azure.account.oauth2.client.secret.storagedatabrickshibu.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagedatabrickshibu.dfs.core.windows.net", "https://login.microsoftonline.com/c61f150e-93fa-41be-b0f6-b6f8b0c1220c/oauth2/token")

# COMMAND ----------

import dlt;
import pyspark.sql.functions as F 

@dlt.table(table_properties={"quality":"bronze"})
@dlt.expect_or_drop("valid_employee_id", "Employee_id IS NOT NULL")

def EMPLOYEE_SURVEY_BRONZE():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://employeeengagementsurvey@storagedatabrickshibu.dfs.core.windows.net/")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            F.col("Employee ID").alias("EMPLOYEE_ID"),
            F.col("Survey Date").alias("SURVER_DATE"),
            F.col("Engagement Score").alias("Engagement_Score"),
            F.col("Satisfaction Score").alias("Satisfaction_Score"),
            F.col("Work-Life Balance Score").alias("Work_Life_Balance_Score")
        )
    )

# COMMAND ----------

dlt.create_streaming_table("EMPLOYEE_SURVEY_SILVER")

# COMMAND ----------

dlt.apply_changes(
    source = "EMPLOYEE_SURVEY_BRONZE",
    target = "EMPLOYEE_SURVEY_SILVER",
    keys = ["EMPLOYEE_ID"],
    sequence_by=F.col("processing_time")
)

# COMMAND ----------

@dlt.table(table_properties={"quality": "gold"}, comment="Final survey table")
def EMPLOYEE_SURVEY():
    return(
        dlt.readStream("EMPLOYEE_SURVEY_SILVER")
    )
