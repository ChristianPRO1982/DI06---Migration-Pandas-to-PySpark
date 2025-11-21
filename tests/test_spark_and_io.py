import pytest

pyspark = pytest.importorskip("pyspark")

from notebooks.pipeline import freshkart_io, spark_session


def test_create_spark_session_in_freshkart_io():
    spark = freshkart_io.create_spark_session("TestApp")
    try:
        assert spark.sparkContext.appName == "TestApp"
    finally:
        spark.stop()


def test_create_spark_session_in_spark_session_module():
    spark = spark_session.create_spark_session("AnotherApp")
    try:
        assert spark.sparkContext.appName == "AnotherApp"
    finally:
        spark.stop()


def test_readers_in_freshkart_io(spark, tmp_path):
    customers = tmp_path / "customers.csv"
    customers.write_text("customer_id,city,is_active\n1,Paris,true\n")
    refunds = tmp_path / "refunds.csv"
    refunds.write_text("order_id,amount\n1,5\n")
    orders = tmp_path / "orders_2024-01-05.json"
    orders.write_text("[{\"order_id\":1}]")

    customers_df = freshkart_io.read_customers(spark, str(customers))
    refunds_df = freshkart_io.read_refunds(spark, str(refunds))
    orders_df = freshkart_io.read_orders_for_date(spark, str(tmp_path), "2024-01-05")

    assert customers_df.collect()[0]["city"] == "Paris"
    assert refunds_df.collect()[0]["amount"] == 5
    assert orders_df.collect()[0]["order_id"] == 1
