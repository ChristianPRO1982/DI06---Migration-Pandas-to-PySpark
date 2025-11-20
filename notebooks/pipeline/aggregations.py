# This module contains aggregation logic for the FreshKart pipeline.
# It builds daily aggregates by date, city and channel from cleaned
# order items and refunds.

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    countDistinct,
    to_date,
)


def compute_daily_city_sales(
    items_df: DataFrame,
    refunds_df: DataFrame,
) -> DataFrame:
    """
    Compute daily aggregates by date, city and channel from clean items and refunds.
    """
    items_with_date = (
        items_df
        .withColumn("order_date", to_date(col("created_at")))
        .withColumn("line_revenue_eur", col("qty") * col("unit_price"))
    )

    order_metrics_df = (
        items_with_date
        .groupBy("order_date", "city", "channel", "order_id", "customer_id")
        .agg(
            spark_sum("qty").alias("items_sold"),
            spark_sum("line_revenue_eur").alias("gross_revenue_eur"),
        )
    )

    refunds_per_order_df = (
        refunds_df
        .groupBy("order_id")
        .agg(
            (-spark_sum(col("amount"))).alias("refunds_eur")
        )
    )

    order_metrics_with_refunds = (
        order_metrics_df
        .join(refunds_per_order_df, on="order_id", how="left")
        .fillna({"refunds_eur": 0.0})
    )

    daily_city_sales_df = (
        order_metrics_with_refunds
        .groupBy("order_date", "city", "channel")
        .agg(
            countDistinct("order_id").alias("orders_count"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_sum("items_sold").alias("items_sold"),
            spark_sum("gross_revenue_eur").alias("gross_revenue_eur"),
            spark_sum("refunds_eur").alias("refunds_eur"),
        )
        .withColumn(
            "net_revenue_eur",
            col("gross_revenue_eur") + col("refunds_eur"),
        )
        .select(
            col("order_date").alias("date"),
            "city",
            "channel",
            "orders_count",
            "unique_customers",
            "items_sold",
            "gross_revenue_eur",
            "refunds_eur",
            "net_revenue_eur",
        )
    )

    return daily_city_sales_df
