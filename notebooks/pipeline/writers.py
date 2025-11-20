# This module contains output writers for the FreshKart pipeline.
# It is responsible for turning Spark DataFrames into stable, readable
# artefacts for downstream consumers (CSV files, etc.).

from datetime import datetime
from pathlib import Path
import shutil

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as spark_round

from .config import OUTPUT_DIR, DATE_INPUT_FORMAT, DATE_OUTPUT_FORMAT


def write_daily_summary_csv(
    daily_city_sales_df: DataFrame,
    date_str: str,
) -> Path:
    """
    Write daily sales metrics for a given date to a single CSV file.
    The file uses ';' as a delimiter and rounds monetary values to two decimals.
    Returns the path to the created CSV file.
    """
    date_obj = datetime.strptime(date_str, DATE_INPUT_FORMAT)
    formatted_date = date_obj.strftime(DATE_OUTPUT_FORMAT)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    filtered_df = daily_city_sales_df.filter(col("date") == date_str)

    rounded_df = (
        filtered_df
        .withColumn("gross_revenue_eur", spark_round(col("gross_revenue_eur"), 2))
        .withColumn("refunds_eur", spark_round(col("refunds_eur"), 2))
        .withColumn("net_revenue_eur", spark_round(col("net_revenue_eur"), 2))
    )

    tmp_dir = OUTPUT_DIR / f"_tmp_daily_summary_{formatted_date}"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    (
        rounded_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(str(tmp_dir))
    )

    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No part CSV file found in {tmp_dir}")

    final_path = OUTPUT_DIR / f"daily_summary_{formatted_date}.csv"
    shutil.move(str(part_files[0]), final_path)

    shutil.rmtree(tmp_dir)

    return final_path
