# This module centralizes the creation of a configured SparkSession
# for the FreshKart daily pipeline. Other modules should import and
# reuse this function instead of building their own SparkSession.

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "FreshKartDailyPipeline") -> SparkSession:
    """
    Create and return a SparkSession for the FreshKart pipeline.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    return spark
