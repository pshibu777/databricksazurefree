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
def EMPLOYEE_DF_RAW_BRONZE():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://employeedata@storagedatabrickshibu.dfs.core.windows.net/")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(comment="Append only orders with valid timestamps", table_properties={"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("StartDate") > F.lit("2018-01-01").cast("date"))
@dlt.expect_or_drop("valid_employee_id", "Employee_id IS NOT NULL")
def EMPLOYEE_DF_RAW_BRONZE_CLEAN():
    return (
        dlt.read_stream("EMPLOYEE_DF_RAW_BRONZE")
        .select(
            F.col("EmpID").cast("int").alias("Employee_id"),
            F.col("FirstName").alias("First_name"),
            F.col("LastName").alias("Last_name"),
            F.to_date(F.col("StartDate"), "dd-MMM-yy").alias("StartDate"),
            F.to_date(F.col("ExitDate"), "dd-MMM-yy").alias("ExitDate"),
            F.col("Title"),
            F.col("Supervisor"),
            F.col("ADEmail"),
            F.col("BusinessUnit"),
            F.col("EmployeeStatus"),
            F.col("EmployeeType"),
            F.col("PayZone"),
            F.col("EmployeeClassificationType"),
            F.col("TerminationType"),
            F.col("TerminationDescription"),
            F.col("DepartmentType"),
            F.col("Division"),
            F.col("DOB"),
            F.col("State"),
            F.col("JobFunctionDescription"),
            F.col("GenderCode"),
            F.col("LocationCode"),
            F.col("RaceDesc"),
            F.col("MaritalDesc"),
            F.col("PerformanceScore"),
            F.col("CurrentEmployeeRating"),
            F.col("processing_time")
        )
    )

# COMMAND ----------

dlt.create_streaming_table("EMPLOYEE_DF_SILVER")

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

dlt.apply_changes(
    source="EMPLOYEE_DF_RAW_BRONZE_CLEAN",
    target="EMPLOYEE_DF_SILVER",
    keys=["Employee_id"],
    sequence_by=F.col("processing_time"))

# COMMAND ----------

@dlt.table(table_properties={"quality": "gold"}, comment="Final employee table")
def EMPLOYEE_DF():   
    employee_silver = dlt.read("EMPLOYEE_DF_SILVER")
    employee_training_silver = dlt.read("EMPLOYEE_TRAINING_DF_SILVER")

    return (
        employee_silver.alias("A")
        .join(
            employee_training_silver.alias("B"),
            F.col("A.Employee_id") == F.col("B.Employee_ID"),
            "left"
        )
        .select(
            F.col("A.*"),
            F.col("B.Training_Date"),
            F.col("B.Training_Program_Name"),
            F.col("B.Training_Type"),
            F.col("B.Training_Outcome"),
            F.col("B.Location"),
            F.col("B.Trainer"),
            F.col("B.Training_Duration_days"),
            F.col("B.Training_Cost")
        )
    )
