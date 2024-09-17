-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Create and Govern Data Objects Effectively with Unity Catalog in Databricks
-- MAGIC
-- MAGIC With Unity calalog you can create calalogs, schemas, tables, view and user-defined functions providing a robust framework to utilize the data assets. We are going to control access these objects with permission by ensuring that authorized user can only view or modify sensitive information. Using dynamic views unity calalog can provide protection in row-level and column-level in tables. exploring grants and revoke on various objects with unity calalog. We are going to use EV Global sales data set.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create a new catalog and select it as default catalog:
-- MAGIC
-- MAGIC First, we need to create a new catalog in our metastore. Run the create statement. the USE statement to select a default schema.
-- MAGIC

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS sales_calalog; 

-- COMMAND ----------

USE CATALOG sales_calalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create and use the new schema:
-- MAGIC Next, let's create a schema in this new catalog. Because we're now using a unique catalog that is isolated from the rest of the metastore. also set this as the default schema.
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS VEHICAL_SALES;

-- COMMAND ----------

USE SCHEMA VEHICAL_SALES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Set up tables and views:
-- MAGIC Under the schema let create a table name as EV_SALES inject data from azure storage account.
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS EV_SALES
USING CSV
OPTIONS (
  PATH 'abfss://unity-matestore-eastus@genstoragedatabrickshibu.dfs.core.windows.net/EV_SALES/IEA Global EV Data 2024.csv',
  HEADER TRUE,
  DELIMITER ','
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Grant access to data objects: 
-- MAGIC By default, unity catalog provides an explicit permission model. No permission is provided or inherited from containing elements. In order to access the data object lets us **USAGE** statement for permission to the user on all container elements which are catalog and schema. **SELECT** on the data objects such as tables and view.
-- MAGIC

-- COMMAND ----------

GRANT USAGE ON CATALOG sales_calalog TO `account users`;

-- COMMAND ----------

GRANT USAGE ON SCHEMA VEHICAL_SALES TO `account users`;

-- COMMAND ----------

GRANT SELECT ON TABLE EV_SALES TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create and grant access to a user-defined function:
-- MAGIC User-defined functions can be create within schema in unity catalog. Here we will create a function to mask the data except the last two character in the string. 
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask_function(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT mask_function('sensitive data') AS data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use **EXECUTE** statement to allow member to account user to run the function.

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION mask_function TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Using dynamic views to protect table column and rows:
-- MAGIC Dynamic views provide the ability to fine-grained access control of columns and rows within table. With partially obscure we can provide column values or redact them entirely. Also, we can omit the rows based on specific criteria.
-- MAGIC
-- MAGIC ###For access control we can use the functions below:
-- MAGIC
-- MAGIC •	**current_user()**: returns the email address of the user querying the view.
-- MAGIC
-- MAGIC •	**is_account_group_member()**: returns TRUE if the user querying the view is a member of the specified group.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###1.	Redact columns:
-- MAGIC Let’s create a view name ev_sales_view use **is_account_group_member()** function.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW ev_sales_view AS
SELECT 
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
  ELSE region 
  END AS REGION,
  CASE WHEN 
    is_account_group_member('account users') THEN 'REDACTED'
  ELSE parameter
  END AS PARAMETER,
mode,
powertrain,
year,
unit,
value
FROM ev_sales;

-- COMMAND ----------

GRANT SELECT ON VIEW ev_sales_view to `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2.	Restrict rows:
-- MAGIC If you want to restrict few rows, we can simply filter out the row based on the condition. Using **WHERE** clause we can apply the function **is_account_group_member()** and create a view. Here we will be filtering out the year which are less than 2018 and display to the user.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW ev_sales_view AS
SELECT 
region,
parameter,
mode,
powertrain,
year,
unit,
value
FROM ev_sales
where 
CASE WHEN
    is_account_group_member('account users') THEN year < 2018
    ELSE TRUE
  END;

-- COMMAND ----------

GRANT SELECT ON VIEW ev_sales_view to `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3.	Data masking:
-- MAGIC Dynamic views final use case is data masking. Masking is similar to redacted columns. Redacted replace the entire column, Masking displays some of the data rather than replacing the entirely. Here we will use **_mask()_** function to manipulate the data for the column region. 
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW ev_sales_view AS
SELECT 
  CASE WHEN
    is_account_group_member('account users') THEN mask(region)
  ELSE region 
  END AS REGION,
  parameter,
mode,
powertrain,
year,
unit,
value
FROM ev_sales;

-- COMMAND ----------

GRANT SELECT ON VIEW ev_sales_view to `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explore permissions:
-- MAGIC Using **SHOW GRANTS** we can view the permissions for the user for the catalog, schemas, table or views.
-- MAGIC

-- COMMAND ----------

SHOW GRANTS ON VIEW ev_sales_view;

-- COMMAND ----------

SHOW GRANTS ON TABLE ev_sales;

-- COMMAND ----------

  SHOW GRANTS ON SCHEMA vehical_sales;

-- COMMAND ----------

SHOW GRANTS ON CATALOG sales_calalog;

-- COMMAND ----------

SHOW GRANTS ON FUNCTION mask_function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Revoke access:
-- MAGIC No data governance platform would be complete without the ability to revoke previously issued grants.
-- MAGIC

-- COMMAND ----------

REVOKE EXECUTE ON FUNCTION mask_function FROM `account users`

-- COMMAND ----------

SHOW GRANTS ON FUNCTION mask_function

-- COMMAND ----------

REVOKE USAGE ON CATALOG sales_calalog FROM `account users`;
