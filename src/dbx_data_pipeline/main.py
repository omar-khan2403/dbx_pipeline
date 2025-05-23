from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def check_null_ids(df: DataFrame, id_col: str, table_name: str) -> DataFrame:
    """
    Check for null IDs in the DataFrame and return a new DataFrame with invalid data.
    """

    if df is None:
        raise ValueError("DataFrame is None")
    if id_col is None:
        raise ValueError("ID column is None")

    #  get null id rows and return them with metadata
    null_id_rows = df.filter(df[id_col].isNull())

    # turn columns into single json object
    null_id_rows = null_id_rows.select(F.to_json(F.struct("*")).alias("invalid_data"))
    # add metadata columns
    invalid_df = (
        null_id_rows.withColumn("table_name", F.lit(table_name))
        .withColumn("error_reason", F.lit("Missing ID"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    return invalid_df
