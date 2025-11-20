from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config import (
    STATIC_DIR,
    INPUT_DIR,
    ORDERS_PREFIX,
    ORDERS_EXTENSION,
    DATE_INPUT_FORMAT,
)


def read_customers(spark: SparkSession) -> DataFrame:
    """
    Read the customers static CSV file into a Spark DataFrame.
    """
    path = STATIC_DIR / "customers.csv"
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(path))
    )


def read_refunds(spark: SparkSession) -> DataFrame:
    """
    Read the refunds static CSV file into a Spark DataFrame.
    """
    path = STATIC_DIR / "refunds.csv"
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(path))
    )


def build_orders_file_path(date_str: str) -> Path:
    """
    Build the full path to a daily orders JSON file from a date string.
    """
    date_obj = datetime.strptime(date_str, DATE_INPUT_FORMAT)
    filename = f"{ORDERS_PREFIX}{date_obj.strftime(DATE_INPUT_FORMAT)}{ORDERS_EXTENSION}"
    return INPUT_DIR / filename


def read_orders_for_date(spark: SparkSession, date_str: str) -> DataFrame:
    """
    Read the daily orders JSON file for a given date (YYYY-MM-DD) into a Spark DataFrame.
    """
    path = build_orders_file_path(date_str)

    if not path.exists():
        raise FileNotFoundError(f"Orders file not found for date {date_str}: {path}")

    return (
        spark.read
        .option("multiline", "true")
        .json(str(path))
    )
