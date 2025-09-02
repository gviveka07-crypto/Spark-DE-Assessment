from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get the spark session

    This function assumes you are using a locally running spark connect server.
    If you choose to use a different spark configuration, feel free to modify
    the function.

    Returns
    -------
    SparkSession
        An active spark session
    """

    return SparkSession.builder.remote("sc://localhost").getOrCreate()
