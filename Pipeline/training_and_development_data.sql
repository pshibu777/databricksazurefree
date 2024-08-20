-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE EMPLOYEE_TRAINING_RAW_BRONZE
AS 
SELECT 
    current_timestamp() AS processing_time, 
    input_file_name() AS source_file, 
    CAST(`Employee ID` AS INT) AS Employee_ID, 
    to_date(`Training Date` , 'dd-MMM-yy') AS Training_Date, 
    `Training Program Name` AS Training_Program_Name, 
    `Training Type` AS Training_Type, 
    `Training Outcome` AS Training_Outcome, 
    Location, 
    Trainer, 
    CAST(`Training Duration(Days)` AS INT) AS Training_Duration_days, 
    CAST(`Training Cost` AS DECIMAL(10, 2)) AS Training_Cost
FROM cloud_files(
    'dbfs:/FileStore/tables/training_and_development_data',
    'csv',
    map(
        "cloudFiles.inferColumnTypes", "true"
    )
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE EMPLOYEE_TRAINING_SILVER
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
    Employee_ID, 
    Training_Date, 
    Training_Program_Name, 
    Training_Type, 
    Training_Outcome, 
    Location, 
    Trainer, 
    Training_Duration_days, 
    Training_Cost,
    processing_time,
    source_file
FROM STREAM(LIVE.EMPLOYEE_TRAINING_RAW_BRONZE);

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE EMPLOYEE_TRAINING
COMMENT "Final EMPLOYEE TRAINING table"
TBLPROPERTIES ("quality" = "gold")
AS (
  SELECT 
    *
  FROM LIVE.EMPLOYEE_TRAINING_SILVER
);

