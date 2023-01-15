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

-- All tables in bronze layer

-- bronze_db_retail.active_promotions
-- bronze_db_retail.company_employees
-- bronze_db_retail.customers
-- bronze_db_retail.loyalty_segments
-- bronze_db_retail.products
-- bronze_db_retail.promotions
-- bronze_db_retail.purchase_orders
-- bronze_db_retail.sales_orders
-- bronze_db_retail.sales_stream
-- bronze_db_retail.suppliers

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze_db_retail;
CREATE DATABASE IF NOT EXISTS silver_db_retail;
CREATE DATABASE IF NOT EXISTS gold_db_retail;

-- COMMAND ----------

-- creating silver layer PART 1


--1
create or replace table silver_db_retail.active_promotions as
select 
cast (promo_customer as long), promo_item,  
cast (promo_disc as int), 
promo_id,
cast(from_unixtime(promo_datetime) as timestamp) as promo_datetime, 
promo_qty,
cumsum,
cast(from_unixtime(promo_began) as timestamp) as promo_began,
units_required,
cast (eligible as int),
cast (deadline as DATE)
from bronze_db_retail.active_promotions;
select * from silver_db_retail.active_promotions;


-- 2
create or replace table silver_db_retail.company_employees as
select 
cast (employee_id as bigint), employee_name, department, region, 
cast (employee_key as bigint), 
cast (active_record as int), 
cast (active_record_start as DATE),
cast (active_record_end as DATE)
from bronze_db_retail.company_employees;
select * from silver_db_retail.company_employees;


--3
create or replace table silver_db_retail.customers as
select 
cast (customer_id as bigint), 
cast (tax_id as double), tax_code,customer_name,state,city,unit,region,district,
cast (postcode as double), street, number,
cast (lon as double), 
cast (lat as double),ship_to_address,
cast(from_unixtime(valid_from) as timestamp) as valid_from,
cast(from_unixtime(valid_to) as timestamp) as valid_to,
cast (units_purchased as double),
cast (loyalty_segment as int) 
from bronze_db_retail.customers;
select * from silver_db_retail.customers;

--4
create or replace table silver_db_retail.loyalty_segments as
select 
cast (loyalty_segment_id as int),  loyalty_segment_description,
cast (unit_threshold as int), 
cast (valid_from as DATE),
cast (valid_to as DATE)
from bronze_db_retail.loyalty_segments;
select * from silver_db_retail.loyalty_segments;

--5
create or replace table silver_db_retail.products as
select product_id,product_category, product_name, 
cast (sales_price as double),
cast (EAN13 as bigint), 
cast (EAN5 as bigint),product_unit
from bronze_db_retail.products;
select * from silver_db_retail.products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Creating all Silver layer tables

-- COMMAND ----------

-- creating silver layer PART 2

-- 6
create or replace table silver_db_retail.promotions as
select 
promotion_id,  promotion_type, dollar_discount, 
cast (percent_discount as double), qualifying_products, units_required, free_product_ids, length, 
cast (valid_from as DATE),
cast (valid_to as DATE)
from bronze_db_retail.promotions;
select * from silver_db_retail.promotions;

--7
create or replace table silver_db_retail.purchase_orders as
select 
EAN13 ,EAN5 ,PO, 
cast(from_unixtime(datetime) as timestamp) as datetime,price, product_name, product_unit, purchaser, 
 cast (quantity as double),supplier 

from bronze_db_retail.purchase_orders;
select * from silver_db_retail.purchase_orders;




-- 8 
create or replace table silver_db_retail.sales_orders as
select 
clicked_items,
cast (customer_id as bigint),customer_name,
cast (number_of_line_items as int),
cast(from_unixtime(order_datetime) as timestamp) as order_datetime,order_number,
inline(ordered_products), promo_info
from bronze_db_retail.sales_orders;
select * from silver_db_retail.sales_orders;


-- 9 (needs more work)
create or replace table silver_db_retail.sales_orders as
select * from bronze_db_retail.sales_stream;


-- 10
create or replace table silver_db_retail.suppliers as
select 
cast (SUPPLIER_ID as bigint),
cast (TAX_ID as bigint),supplier_name,state,city,
cast (postcode as double),street,
cast (number as int),
cast (unit as int),region,district,
cast (lon as double),
cast (lat as double),items_provided
from bronze_db_retail.suppliers;
select * from silver_db_retail.suppliers;

-- COMMAND ----------

--select expl as cs from(
--select * from bronze_db_retail.suppliers;
--select * from bronze_db_retail.purchase_orders;-- where EAN13=2198122550193;
--DESCRIBE EXTENDED silver_db_retail.sales_orders;
DESCRIBE EXTENDED bronze_db_retail.sales_stream;
