-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE employee_engagement_survey_bronze
TBLPROPERTIES("quality" = "bronze")
AS SELECT current_timestamp() AS processing_time, 
          input_file_name() AS source_file, 
          cast(`Employee ID` AS INT) AS EMPLOYEE_ID, 
          cast(`Survey Date` AS DATE) AS SURVEY_DATE, 
          cast(`Engagement Score` AS INT) AS Engagement_Score, 
          cast(`Satisfaction Score` AS INT) AS Satisfaction_Score, 
          cast(`Work-Life Balance Score` AS INT) AS Work_Life_Balance_Score
FROM cloud_files(
    'dbfs:/FileStore/tables/employee_engagement_survey_data',
    'csv',
    map(
        "cloudFiles.inferColumnTypes", "true"
    )
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE employee_engagement_survey_silver
TBLPROPERTIES("quality" = "silver");

APPLY CHANGES INTO live.employee_engagement_survey_silver
  FROM STREAM(LIVE.employee_engagement_survey_bronze)
  KEYS (EMPLOYEE_ID)
  SEQUENCE BY processing_time;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE employee_engagement_survey
TBLPROPERTIES("quality" = "gold")
AS (SELECT * FROM live.employee_engagement_survey_silver);
