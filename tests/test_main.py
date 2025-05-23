from dbx_data_pipeline.main import check_null_ids


def test_check_null_ids():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("TestCheckNullIds") \
        .master("local[*]") \
        .getOrCreate()

    # Create a sample DataFrame with null IDs
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("row_update_time", TimestampType(), True)
    ])

    data = [
        (None, "John Doe", "2023-10-01 12:00:00"),
        ("123", "Jane Smith", "2023-10-01 12:00:00")
    ]

    df = spark.createDataFrame(data, schema)

    # Call the function to check for null IDs
    result_df = check_null_ids(df, "id", "test_table")

    # Collect the result
    result_data = result_df.collect()

    # Check if the result DataFrame contains the expected invalid data
    assert len(result_data) == 1
    assert result_data[0]["invalid_data"]["id"] is None