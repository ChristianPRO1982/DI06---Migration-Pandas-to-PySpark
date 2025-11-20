# This module contains all transformation functions for the FreshKart pipeline.
# Each function applies a single business rule or cleaning step.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode


def filter_paid_orders(orders_df: DataFrame) -> DataFrame:
    """
    Keep only orders with payment_status = 'paid'.
    """
    return orders_df.filter(col("payment_status") == "paid")


def join_active_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    Join orders with active customers only and enrich with customer attributes.
    """
    active_customers_df = customers_df.filter(col("is_active") == True)

    joined_df = (
        orders_df.join(
            active_customers_df.select("customer_id", "city", "is_active"),
            on="customer_id",
            how="inner",
        )
    )

    return joined_df


def explode_items(orders_df: DataFrame) -> DataFrame:
    """
    Explode the items array so that each row represents a single order line.
    """
    exploded_df = orders_df.withColumn("item", explode("items"))

    return exploded_df.select(
        col("order_id"),
        col("customer_id"),
        col("channel"),
        col("created_at"),
        col("city"),
        col("item.sku").alias("sku"),
        col("item.qty").alias("qty"),
        col("item.unit_price").alias("unit_price"),
    )


def filter_negative_prices(items_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split items into valid rows (unit_price >= 0) and rejected rows (unit_price < 0).
    Returns a tuple: (clean_items_df, rejected_items_df).
    """
    rejected_df = items_df.filter(col("unit_price") < 0)
    clean_df = items_df.filter(col("unit_price") >= 0)

    return clean_df, rejected_df