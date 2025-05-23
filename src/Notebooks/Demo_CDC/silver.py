# Databricks notebook source
from dbx_data_pipeline.main import check_null_ids

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import yaml


dbutils.widgets.text("catalog", "catalog_dev")
dbutils.widgets.text("schema", "schema")
dbutils.widgets.text("config_yml", "")


catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
config_yml_path = dbutils.widgets.get("config_yml")

# COMMAND ----------

#load yml
with open(config_yml_path, "r") as file:
    config = yaml.safe_load(file)

bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
quarantine_table = config["quarantine_table"]

# COMMAND ----------

bronze_df = (
    spark.table(f"{catalog}.{schema}.{bronze_table}")
)
window_spec = Window.partitionBy("id").orderBy(F.col("row_update_time").desc())
latest_snapshot = (
    bronze_df.withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num = 1")
    .drop("row_num")
)

# dq check on latest snapshot data to ensure no null ids
invalid_data = check_null_ids(latest_snapshot, "id", bronze_table)

# append invalid data to a separate table for review
invalid_data.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.{quarantine_table}")


# COMMAND ----------

# configure silver table for merging
silver = DeltaTable.forName(spark, f"{catalog}.{schema}.{silver_table}")


# Merge the latest snapshot into the silver table
silver.alias("target").merge(
    source=latest_snapshot.alias("source"),
    condition="target.id = source.id"
).whenNotMatchedInsert(
    values={
        "id": "source.id",
        "name": "source.name",
        "row_update_time": "source.row_update_time",
        "is_current": F.lit(True),
        "end_date": F.lit(None).cast("timestamp"),
    }
).whenMatchedUpdate(
    condition="source.row_update_time > target.row_update_time",
    set={
        "is_current": F.lit(False),
        "end_date": F.current_timestamp(),
    }
).execute()

