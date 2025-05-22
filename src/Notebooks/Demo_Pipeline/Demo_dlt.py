import dlt
import pyspark.sql.functions as F

source_data = spark.conf.get("bundle.datasource")

# Create the target bronze table
dlt.create_streaming_table("cdc_bronze", comment="New data incrementally ingested from cloud object storage landing zone")

# Create an Append Flow to ingest the raw data into the bronze table
@dlt.append_flow(
  target = "data_bronze",
  name = "cbronze_ingest_flow"
)
def bronze_ingest_flow():
  return (
      spark.readStream
          .format("cloudFiles") # using autoloader to ingest data from cloud object storage location
          .option("cloudFiles.format", "json")
          .option("inferTimeStamp", "true")
          .option("timestampFormat", "yyyy/MM/dd HH:mm:ss.SSSSSSSSS")
          .option("cloudFiles.schemaLocation", "/tmp/bronze_schema") # load schema from location, delta tables have issues with spaces in column names
          .load(source_data)
          .withColumn("_metadata", F.col("_metadata"))
  )

@dlt.table(
  name = "data_silver",
  comment = "Silver table with data quality checks"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
def silver():
    bronze_table = dlt.readStream("data_bronze")
    # Perform cleaning and simple transformations
    silver_table = bronze_table.select(
        F.col("id").cast("int").alias("id"),
        F.col("name").cast("string").alias("name"),
    )
    # Add a new column with the current date

    return silver_table






