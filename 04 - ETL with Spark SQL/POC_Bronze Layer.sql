-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Extract and Load Data Lab
-- MAGIC 
-- MAGIC In this lab, you will extract and load raw data from JSON files into a Delta table.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create an external table to extract data from JSON files
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.5L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Creating required Schema

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze_db_retail;
CREATE DATABASE IF NOT EXISTS silver_db_retail;
CREATE DATABASE IF NOT EXISTS gold_db_retail;

-- COMMAND ----------

--show databases;
describe database bronze_db_retail;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #View list of datasets 
-- MAGIC 
-- MAGIC 
-- MAGIC dataset_path = "dbfs:/databricks-datasets/retail-org/sales_stream"
-- MAGIC print(dataset_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Creating all bronze layer tables

-- COMMAND ----------

--all bronze schema table creation statements are defined here: PART 1
--1
-- CREATE OR REPLACE TABLE bronze_db_retail.active_promotions AS SELECT * FROM parquet.`dbfs:/databricks-datasets/retail-org/active_promotions/active_promotions.parquet`;

--2
-- drop table if exists bronze_db_retail.company_employees;
-- drop view if exists company_employees;
-- CREATE OR REPLACE TEMP VIEW company_employees
-- USING CSV
-- OPTIONS (
--   path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/company_employees/company_employees.csv",
--   header  "true");
-- --LOCATION 
-- CREATE TABLE bronze_db_retail.company_employees AS
--   SELECT * FROM company_employees;

--3
-- drop table bronze_db_retail.customers;
-- CREATE table bronze_db_retail.customers
-- USING CSV
-- OPTIONS (
--   header = "true")
-- LOCATION "dbfs:/databricks-datasets/retail-org/customers/customers.csv"


--4 (where schema cannot be infered, create view and then a table from view)
-- drop table if exists bronze_db_retail.loyalty_segments;
-- drop view if exists loyalty_segments;
-- CREATE OR REPLACE TEMP VIEW loyalty_segments
-- --(loyalty_segment_id string, loyalty_segment_description string, unit_threshold string, valid_from string, valid_to string )
-- USING CSV
-- OPTIONS (
--   path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/loyalty_segments/loyalty_segment.csv",
--   header "true"
--   );
-- CREATE TABLE bronze_db_retail.loyalty_segments AS
--   SELECT * FROM loyalty_segments;

-- COMMAND ----------

--all bronze schema table creation statements are defined here: PART 2

--5 (where schema cannot be infered, create view and then a table from view)
-- drop table if exists bronze_db_retail.products;
-- drop view if exists products;
-- CREATE OR REPLACE TEMP VIEW products
-- --(loyalty_segment_id string, loyalty_segment_description string, unit_threshold string, valid_from string, valid_to string )
-- USING CSV
-- OPTIONS (
--   path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/products/products.csv",
--   header "true",
--   delimiter ";"
--   );
-- CREATE TABLE bronze_db_retail.products AS
--   SELECT * FROM products;
  
  
--6 (where schema cannot be infered, create view and then a table from view)
-- drop table if exists bronze_db_retail.promotions;
-- drop view if exists promotions;
-- CREATE OR REPLACE TEMP VIEW promotions
-- --(loyalty_segment_id string, loyalty_segment_description string, unit_threshold string, valid_from string, valid_to string )
-- USING CSV
-- OPTIONS (
--   path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/promotions/promotions.csv",
--   header "true"
--   --delimiter ";"
--   );
-- CREATE TABLE bronze_db_retail.promotions AS
--   SELECT * FROM promotions;

--7
-- drop table if exists bronze_db_retail.purchase_orders;
-- CREATE TABLE bronze_db_retail.purchase_orders
-- USING xml
-- OPTIONS (path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/purchase_orders/purchase_orders.xml", rowTag "purchase_item", inferSchema "true")

--8
-- drop table if exists bronze_db_retail.sales_orders;
-- CREATE TABLE bronze_db_retail.sales_orders as
-- select * from json.`dbfs:/databricks-datasets/retail-org/sales_orders/`;


--9
-- drop table if exists bronze_db_retail.sales_stream;
-- CREATE TABLE bronze_db_retail.sales_stream as
-- select * from json.`dbfs:/databricks-datasets/retail-org/sales_stream/sales_stream.json`;

--10
-- drop table if exists bronze_db_retail.suppliers;
-- drop view if exists suppliers;
-- CREATE OR REPLACE TEMP VIEW suppliers
-- --(loyalty_segment_id string, loyalty_segment_description string, unit_threshold string, valid_from string, valid_to string )
-- USING CSV
-- OPTIONS (
--   path "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/suppliers/suppliers.csv",
--   header "true"
--   --delimiter ";"
--   );
-- CREATE TABLE bronze_db_retail.suppliers AS
--   SELECT * FROM suppliers;

-- COMMAND ----------



-- COMMAND ----------

--select expl as cs from(
--select * from bronze_db_retail.suppliers;
--select * from bronze_db_retail.purchase_orders;-- where EAN13=2198122550193;
DESCRIBE EXTENDED bronze_db_retail.purchase_orders;

-- COMMAND ----------

DESCRIBE EXTENDED bronze_db_retail.sales_orders;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.head("dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/loyalty_segments/loyalty_segment.csv")
-- MAGIC p="/dbfs/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/sales_stream/sales_stream.json/"
-- MAGIC # f = open(p, "r")
-- MAGIC # print(f)
-- MAGIC 
-- MAGIC with  open(p, "r") as f_read:
-- MAGIC   for line in f_read:
-- MAGIC     print(line)
-- MAGIC spark_p="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/retail-org/sales_stream/"
-- MAGIC # df = spark.read.option("header","true").option("delimiter",";").json(spark_p)
-- MAGIC # display(df)
-- MAGIC # print(df.dtypes)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
