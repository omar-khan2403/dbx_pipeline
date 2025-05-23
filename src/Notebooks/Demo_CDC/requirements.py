# Databricks notebook source
import yaml

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("group", "")
dbutils.widgets.text("config_yml", "")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
group = dbutils.widgets.get("group")
config_yml_path = dbutils.widgets.get("config_yml")

# COMMAND ----------

# load yml file with sharepoint site names
with open(config_yml_path, "r") as file:
    config = yaml.safe_load(file)


# COMMAND ----------

# create schemas
schema = dbutils.widgets.get("schema")

query = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};"
print(query)
spark.sql(query)

select_query = f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{group}`; "
print(query)
spark.sql(select_query)

use_query = f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{group}`; "
print(query)
spark.sql(use_query)


# COMMAND ----------

# Table DDL
bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
quarantine_table = config["quarantine_table"]

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{bronze_table} (
        id STRING,
        name STRING,
        row_update_time TIMESTAMP
    )
    USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{silver_table} (
    id STRING,
    name STRING,
    row_update_time TIMESTAMP,
    is_current BOOLEAN,
    start_date TIMESTAMP,
    end_date TIMESTAMP
) USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{silver_table} (
    id STRING,
    name STRING,
    row_update_time TIMESTAMP,
    is_current BOOLEAN,
    start_date TIMESTAMP,
    end_date TIMESTAMP
) USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{quarantine_table} (
    invalid_data STRUCT,
    table_name STRING,
    error_reason STRING,
    ingested_at TIMESTAMP,
) USING DELTA
"""
)
