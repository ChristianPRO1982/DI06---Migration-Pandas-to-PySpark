import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import Row

from notebooks.pipeline import writers


def test_write_daily_summary_csv_creates_single_file(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(writers, "OUTPUT_DIR", tmp_path)
    df = spark.createDataFrame(
        [
            Row(date="2024-01-06", city="Paris", channel="web", orders_count=1, unique_customers=1, items_sold=2, gross_revenue_eur=13.234, refunds_eur=-1.234, net_revenue_eur=12.0),
            Row(date="2024-01-07", city="Lyon", channel="store", orders_count=1, unique_customers=1, items_sold=1, gross_revenue_eur=10.0, refunds_eur=0.0, net_revenue_eur=10.0),
        ]
    )

    final_path = writers.write_daily_summary_csv(df, "2024-01-06")

    assert final_path.name == "daily_summary_20240106.csv"
    assert final_path.exists()

    content = final_path.read_text().strip().splitlines()
    assert content[0] == "date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur"
    assert "2024-01-06;Paris;web;1;1;2;13.23;-1.23;12.0" in content[1]
