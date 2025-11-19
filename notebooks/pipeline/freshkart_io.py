from pyspark.sql import SparkSession, DataFrame


def create_spark_session(app_name: str = "FreshKartDailyPipeline") -> SparkSession:
    """
    Create and return a SparkSession for the FreshKart daily pipeline.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def read_customers(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the customers CSV file and return a DataFrame.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(path)
    )
    return df


def read_refunds(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the refunds CSV file and return a DataFrame.
    """
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(path)
    )
    return df


def read_orders_for_date(spark: SparkSession, base_dir: str, date_str: str) -> DataFrame:
    """
    Read the orders JSON file for a given date (YYYY-MM-DD) and return a DataFrame.
    """
    file_path = f"{base_dir}/orders_{date_str}.json"
    df = (
        spark.read
        .option("multiline", "true")
        .json(file_path)
    )
    return df
