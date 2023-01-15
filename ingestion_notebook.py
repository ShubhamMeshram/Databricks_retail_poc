# Databricks notebook source
"""
The purpose of this notebook is to ingest retail data
"""

# COMMAND ----------

#p= "dbfs:/mnt/dbacademy-users/shubham.meshram@tredence.com/data-engineering-with-databricks/lab/external"
#display(dbutils.fs.ls(p))

spark.sql("CREATE TABLE default.retail_table OPTIONS (PATH 'dbfs:/databricks-datasets/retail-org/')")
