-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE EMPLOYEE_RAW_BRONZE AS
SELECT current_timestamp() processing_time, input_file_name() source_file, *
from cloud_files(
    'dbfs:/FileStore/tables/employee',
    'csv',
    map("cloudFiles.inferColumnTypes", "true")
);

-- COMMAND ----------

CREATE STREAMING TABLE EMPLOYEE_SILVER
(CONSTRAINT valid_date EXPECT (STARTDATE > '2018-01-01') ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    CAST(EmpID as INT) AS EMPLOYEE_ID,
    FirstName  AS FIRST_NAME,
    LastName AS LAST_NAME,
    to_date(StartDate, 'dd-MMM-yy') AS StartDate, 
    to_date(ExitDate, 'dd-MMM-yy') AS ExitDate,  
    Title,
    Supervisor,
    ADEmail AS AD_EMAIL,
    BusinessUnit,
    EmployeeStatus,
    EmployeeType,
    PayZone,
    EmployeeClassificationType,
    TerminationType,
    TerminationDescription,
    DepartmentType,
    Division,
    DOB,
    State,
    JobFunctionDescription,
    GenderCode,
    LocationCode,
    RaceDesc,
    MaritalDesc,
    PerformanceScore,
    CurrentEmployeeRating, 
    processing_time 
FROM STREAM(LIVE.EMPLOYEE_RAW_BRONZE);

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE EMPLOYEE
COMMENT "Final employee table"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT * FROM LIVE.EMPLOYEE_SILVER ;
