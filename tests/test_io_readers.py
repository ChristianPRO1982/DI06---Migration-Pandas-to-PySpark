import pytest

pyspark = pytest.importorskip("pyspark")

from notebooks.pipeline import io_readers


def test_read_customers_loads_csv(tmp_path, spark, monkeypatch):
    monkeypatch.setattr(io_readers, "STATIC_DIR", tmp_path)
    customers_csv = tmp_path / "customers.csv"
    customers_csv.write_text("customer_id,city,is_active\n1,Paris,true\n")

    df = io_readers.read_customers(spark)

    assert df.collect()[0]["city"] == "Paris"


def test_read_refunds_loads_csv(tmp_path, spark, monkeypatch):
    monkeypatch.setattr(io_readers, "STATIC_DIR", tmp_path)
    refunds_csv = tmp_path / "refunds.csv"
    refunds_csv.write_text("order_id,amount\n1,5.0\n")

    df = io_readers.read_refunds(spark)

    assert df.collect()[0]["amount"] == 5.0


def test_build_orders_file_path_formats_date(monkeypatch, tmp_path):
    monkeypatch.setattr(io_readers, "INPUT_DIR", tmp_path)
    path = io_readers.build_orders_file_path("2024-01-03")
    assert path == tmp_path / "orders_2024-01-03.json"


def test_read_orders_for_date_raises_when_missing(monkeypatch, tmp_path, spark):
    monkeypatch.setattr(io_readers, "INPUT_DIR", tmp_path)
    with pytest.raises(FileNotFoundError):
        io_readers.read_orders_for_date(spark, "2024-01-03")


def test_read_orders_for_date_reads_json(monkeypatch, tmp_path, spark):
    monkeypatch.setattr(io_readers, "INPUT_DIR", tmp_path)
    path = io_readers.build_orders_file_path("2024-01-04")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[{\"order_id\": 1}]")

    df = io_readers.read_orders_for_date(spark, "2024-01-04")

    assert df.collect()[0]["order_id"] == 1
