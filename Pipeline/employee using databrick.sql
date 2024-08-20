-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE EMPLOYEE_RAW_BRONZE AS 
SELECT current_timestamp() AS processing_time, 
       input_file_name() AS source_file, 
       *
FROM cloud_files(
    'dbfs:/FileStore/tables/employee',
    'csv',
    map("cloudFiles.inferColumnTypes", "true")
);


-- COMMAND ----------

CREATE STREAMING TABLE EMPLOYEE_BRONZE_CLEAN
(CONSTRAINT valid_date EXPECT (STARTDATE > '2018-01-01') ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "bronze")
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

CREATE OR REFRESH STREAMING TABLE EMPLOYEE_SILVER
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO LIVE.EMPLOYEE_SILVER
  FROM STREAM(LIVE.EMPLOYEE_BRONZE_CLEAN)
  KEYS (EMPLOYEE_ID)
  SEQUENCE BY processing_time

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE EMPLOYEE
COMMENT "Final employee table"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    A.*, 
    B.Training_Date, 
    B.Training_Program_Name, 
    B.Training_Type, 
    B.Training_Outcome, 
    B.Location, 
    B.Trainer, 
    B.Training_Duration_days, 
    B.Training_Cost 
FROM 
    LIVE.EMPLOYEE_SILVER A 
LEFT JOIN 
    LIVE.EMPLOYEE_TRAINING_SILVER B 
ON 
    A.EMPLOYEE_ID = B.Employee_ID;
