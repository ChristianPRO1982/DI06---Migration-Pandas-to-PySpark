import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import Row
from pyspark.sql.functions import col

from notebooks.pipeline import transformations


def test_filter_paid_orders(spark):
    df = spark.createDataFrame(
        [
            Row(order_id=1, payment_status="paid"),
            Row(order_id=2, payment_status="pending"),
        ]
    )

    result = transformations.filter_paid_orders(df).select("order_id").collect()

    assert [row.order_id for row in result] == [1]


def test_join_active_customers_filters_inactive(spark):
    orders_df = spark.createDataFrame(
        [Row(order_id=1, customer_id=100), Row(order_id=2, customer_id=200)]
    )
    customers_df = spark.createDataFrame(
        [
            Row(customer_id=100, city="Paris", is_active=True),
            Row(customer_id=200, city="Lyon", is_active=False),
        ]
    )

    result = transformations.join_active_customers(orders_df, customers_df)

    assert result.select("order_id", "city").collect() == [Row(order_id=1, city="Paris", is_active=True)]


def test_explode_items_expands_rows(spark):
    orders_df = spark.createDataFrame(
        [
            Row(
                order_id=1,
                customer_id=10,
                channel="web",
                created_at="2024-01-01",
                city="Paris",
                items=[Row(sku="A", qty=2, unit_price=5.0), Row(sku="B", qty=1, unit_price=10.0)],
            )
        ]
    )

    result = transformations.explode_items(orders_df)
    collected = result.orderBy(col("sku")).collect()

    assert collected == [
        Row(order_id=1, customer_id=10, channel="web", created_at="2024-01-01", city="Paris", sku="A", qty=2, unit_price=5.0),
        Row(order_id=1, customer_id=10, channel="web", created_at="2024-01-01", city="Paris", sku="B", qty=1, unit_price=10.0),
    ]


def test_filter_negative_prices_splits_frames(spark):
    items_df = spark.createDataFrame(
        [
            Row(order_id=1, unit_price=5.0),
            Row(order_id=2, unit_price=-1.0),
        ]
    )

    clean_df, rejected_df = transformations.filter_negative_prices(items_df)

    assert clean_df.collect() == [Row(order_id=1, unit_price=5.0)]
    assert rejected_df.collect() == [Row(order_id=2, unit_price=-1.0)]
