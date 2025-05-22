from pyspark.sql import SparkSession, DataFrame


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()



def check_null_ids(df: DataFrame) -> DataFrame:
    """
    Check for null IDs in the DataFrame and return a new DataFrame with the null IDs filtered out.
    """
    return df.filter(df.id.isNotNull())


if __name__ == "__main__":
    main()
