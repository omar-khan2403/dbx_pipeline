# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml

spark = SparkSession.getActiveSession()


dbutils.widgets.text("catalog", "catalog_dev")
dbutils.widgets.text("schema", "schema")
dbutils.widgets.text("datasource", "")
dbutils.widgets.text("config_yml", "")


catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_data_path = dbutils.widgets.get("datasource")
config_yml_path = dbutils.widgets.get("config_yml")

# COMMAND ----------

#load yml
with open(config_yml_path, "r") as file:
    config = yaml.safe_load(file)

bronze_table = config["bronze_table"]

# COMMAND ----------

bronze_df = (
    spark.readStream.format(
        "cloudFiles"
    )  # using autoloader to ingest data from cloud object storage location
    .option("cloudFiles.format", "json")
    .option("inferTimeStamp", "true")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss.SSSSSSSSS")
    .option(
        "cloudFiles.schemaLocation", "/tmp/bronze_schema"
    )  # load schema from location, delta tables have issues with spaces in column names
    .load(source_data_path)  # load data from source path
    .withColumn("_metadata", F.col("_metadata"))
)
# Write the data to a bronze table
bronze_df.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", "/tmp/bronze_checkpoint"
).table(f"{catalog}.{schema}.{bronze_table}")  # write to bronze table
