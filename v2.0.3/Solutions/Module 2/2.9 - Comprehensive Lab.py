# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Comprehensive Lab
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC * Demonstrate how to create a complete data engineering workflow in the Databricks Lakehouse Platform

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11

# COMMAND ----------

# ANSWER

# Change the following command to verify that a cluster is running
actual = True
print(f"{actual}")

# COMMAND ----------

assert actual == True, "You need to check the cluster"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a table
# MAGIC Write code in the cell below that creates a Delta table named **`sample_data`** that we will use in this lab.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE TABLE IF NOT EXISTS sample_data;

# COMMAND ----------

expected_table = lambda: spark.table("sample_data")
suite = DA.tests.new("Table Creation Verification")
suite.test_not_none(lambda: expected_table(), "Table \"sample_data\" is created")

suite.display_results()
assert suite


# COMMAND ----------

# MAGIC %md
# MAGIC ##Grant select permission
# MAGIC Grant the ability to run **SELECT** on the table to all account users

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC -- If you would like to use a query to complete this task, you could use:
# MAGIC GRANT SELECT ON TABLE sample_data TO `account users`;

# COMMAND ----------

# Check if the grant was successful
result = spark.sql("SHOW GRANT ON TABLE sample_data")
assert  not result.isEmpty(), "GRANT was not performed correctly. Try again."

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingest Data into the Table
# MAGIC Change "FILL_IN" in the code below so that the cell copies data from the file, `/databricks-datasets/retail-org/promotions/promotions.csv`, into the table, `sample_data`.  
# MAGIC   
# MAGIC Ensure `FORMAT_OPTIONS` have the proper key/value pairs to infer the schema and use the first row in the `.csv` file as the header.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- ANSWER
# MAGIC
# MAGIC COPY INTO sample_data
# MAGIC   FROM '/databricks-datasets/retail-org/promotions/promotions.csv'
# MAGIC   FILEFORMAT = CSV
# MAGIC   FORMAT_OPTIONS ('inferSchema' = 'true', 'header' = 'true')
# MAGIC   COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

suite = DA.tests.new("Data Ingestion")

# Verify data is loaded successfully
expected_table = lambda: spark.table("sample_data")
suite.test_true(lambda: expected_table().count() > 0, "Data loaded into the table")

suite.display_results()
assert suite.passed


# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a Scheduled Job
# MAGIC Configuring this job will require parameters unique to a given user.
# MAGIC
# MAGIC Run the cell below to print out values you'll use to configure your job.
# MAGIC

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# ANSWER
# This portion of the lab requires you to manually configure the job. However, we can use the code below to automatically configure the job using the Databricks Academy (DA) object
DA.create_job_v1()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use the cell below to verify that you configured the job correctly

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Job
# MAGIC Now, run the job that you configured in the previous step.

# COMMAND ----------

# ANSWER
# This portion of the lab requires you to manually run the job. However, we can use the code below to automatically run the job using the Databricks Academy (DA) object.
# This will take a bit of time
DA.start_job()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Double Check Your Work
# MAGIC The cell below will verify that you were able to successfully run the job you created. 

# COMMAND ----------

# Check that the query result is less than 4 rows
query_result = spark.sql("SELECT * FROM sample_data WHERE promotion_type = '4'")
assert query_result.count() > 0, "There must have been a problem with the job run"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!!
# MAGIC You have successfully completed the comprehensive lab!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Clean up Classroom
# MAGIC
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
