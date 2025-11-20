# This module orchestrates the full FreshKart daily pipeline:
# - create the SparkSession
# - load static reference data
# - discover available dates from input files
# - run all transformations and aggregations per date
# - write daily CSV summaries

from datetime import datetime
from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession

from .aggregations import compute_daily_city_sales
from .config import (
    INPUT_DIR,
    ORDERS_EXTENSION,
    ORDERS_PREFIX,
    DATE_INPUT_FORMAT,
)
from .io_readers import (
    read_customers,
    read_refunds,
    read_orders_for_date,
)
from .spark_session import create_spark_session
from .transformations import (
    explode_items,
    filter_negative_prices,
    filter_paid_orders,
    join_active_customers,
)
from .writers import write_daily_summary_csv
from .file_management import move_to_done, move_to_error, move_path_to_error



def list_available_dates() -> List[str]:
    """
    Inspect the input directory and return all available dates
    inferred from orders_YYYY-MM-DD.json file names.
    Any file with an invalid date format is moved to the error folder.
    """
    dates: set[str] = set()

    for path in INPUT_DIR.glob(f"{ORDERS_PREFIX}*{ORDERS_EXTENSION}"):
        name = path.name
        core = name[len(ORDERS_PREFIX) : -len(ORDERS_EXTENSION)]
        try:
            datetime.strptime(core, DATE_INPUT_FORMAT)
        except ValueError:
            move_path_to_error(path)
            print(f"❌ Invalid date in filename, moved to error/: {name}")
            continue
        dates.add(core)

    return sorted(dates)


def process_date(
    spark: SparkSession,
    customers_df: DataFrame,
    refunds_df: DataFrame,
    date_str: str,
) -> None:
    """
    Run the full pipeline for a single date string (YYYY-MM-DD).
    """
    orders_df = read_orders_for_date(spark, date_str)
    paid_df = filter_paid_orders(orders_df)
    joined_df = join_active_customers(paid_df, customers_df)
    items_df = explode_items(joined_df)
    clean_items_df, _rejected_items_df = filter_negative_prices(items_df)
    daily_city_sales_df = compute_daily_city_sales(clean_items_df, refunds_df)
    write_daily_summary_csv(daily_city_sales_df, date_str)


def run_pipeline_for_dates(dates: Iterable[str]) -> None:
    """
    Run the FreshKart pipeline for all given dates.
    Each date is processed independently and errors do not stop the pipeline.
    """
    spark = create_spark_session()
    try:
        customers_df = read_customers(spark)
        refunds_df = read_refunds(spark)

        for date_str in dates:
            print(f"=== Processing date {date_str} ===")
            try:
                process_date(spark, customers_df, refunds_df, date_str)
                move_to_done(date_str)
                print(f"✔ Success: {date_str}")
            except Exception as e:
                move_to_error(date_str)
                print(f"❌ ERROR for {date_str}: {e}")
                continue

    finally:
        spark.stop()


def run() -> None:
    """
    Discover all available dates from input files and run the pipeline.
    """
    dates = list_available_dates()
    run_pipeline_for_dates(dates)


if __name__ == "__main__":
    run()
