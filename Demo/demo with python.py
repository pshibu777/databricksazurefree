# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table
def EMPLOYEE_DF_RAW_BRONZE():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")#
        #.option("cloudFiles.schemaHints", "StartDate DATE, ExitDate DATE")
        .load("dbfs:/FileStore/tables/employee/")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
             "*"
             )
)

# COMMAND ----------

@dlt.table(comment = "Append only orders with valid timestamps", table_properties = {"quality":"silver"})
@dlt.expect_or_fail("valid_date", F.col("StartDate") > F.lit("2018-01-01").cast("date"))
def EMPLOYEE_DF_SILVER():
    return(
        dlt.readStream("EMPLOYEE_DF_RAW_BRONZE")
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
            F.col("CurrentEmployeeRating")
        )
    )

# COMMAND ----------

@dlt.table(table_properties = {"quality" : "gold"})
def employee_df():
    return(
        dlt.read("EMPLOYEE_DF_SILVER")
    )
