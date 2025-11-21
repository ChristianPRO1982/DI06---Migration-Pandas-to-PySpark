import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import Row
from pyspark.sql.functions import col

from notebooks.pipeline import aggregations


def test_compute_daily_city_sales(spark):
    items_df = spark.createDataFrame(
        [
            Row(order_id=1, customer_id=10, channel="web", created_at="2024-01-01", city="Paris", qty=2, unit_price=5.0),
            Row(order_id=1, customer_id=10, channel="web", created_at="2024-01-01", city="Paris", qty=1, unit_price=3.0),
            Row(order_id=2, customer_id=20, channel="store", created_at="2024-01-01", city="Lyon", qty=1, unit_price=10.0),
        ]
    )
    refunds_df = spark.createDataFrame([Row(order_id=1, amount=1.5)])

    result = aggregations.compute_daily_city_sales(items_df, refunds_df)

    paris_row = result.filter(col("city") == "Paris").collect()[0]
    lyon_row = result.filter(col("city") == "Lyon").collect()[0]

    assert paris_row.orders_count == 1
    assert paris_row.items_sold == 3
    assert paris_row.gross_revenue_eur == 13.0
    assert paris_row.refunds_eur == -1.5
    assert paris_row.net_revenue_eur == 11.5

    assert lyon_row.orders_count == 1
    assert lyon_row.items_sold == 1
    assert lyon_row.gross_revenue_eur == 10.0
    assert lyon_row.refunds_eur == 0.0
    assert lyon_row.net_revenue_eur == 10.0
