from types import SimpleNamespace

import pytest

pyspark = pytest.importorskip("pyspark")

from notebooks.pipeline import orchestrator


def test_list_available_dates_filters_invalid(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrator, "INPUT_DIR", tmp_path)
    valid = tmp_path / "orders_2024-01-01.json"
    invalid = tmp_path / "orders_2024-13-01.json"
    other = tmp_path / "orders_2024-01-01.txt"
    valid.write_text("{}")
    invalid.write_text("{}")
    other.write_text("{}")

    moved = []
    monkeypatch.setattr(orchestrator, "move_path_to_error", lambda path: moved.append(path))

    dates = orchestrator.list_available_dates()

    assert dates == ["2024-01-01"]
    assert moved == [invalid]


def test_process_date_calls_pipeline_steps(monkeypatch):
    calls = []

    def record(name):
        def inner(*args, **kwargs):
            calls.append(name)
            return SimpleNamespace()

        return inner

    monkeypatch.setattr(orchestrator, "read_orders_for_date", record("read_orders_for_date"))
    monkeypatch.setattr(orchestrator, "filter_paid_orders", record("filter_paid_orders"))
    monkeypatch.setattr(orchestrator, "join_active_customers", record("join_active_customers"))
    monkeypatch.setattr(orchestrator, "explode_items", record("explode_items"))
    monkeypatch.setattr(orchestrator, "filter_negative_prices", lambda df: (record("filter_negative_prices")(), None))
    monkeypatch.setattr(orchestrator, "compute_daily_city_sales", record("compute_daily_city_sales"))
    monkeypatch.setattr(orchestrator, "write_daily_summary_csv", record("write_daily_summary_csv"))

    orchestrator.process_date(SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), "2024-01-08")

    assert calls == [
        "read_orders_for_date",
        "filter_paid_orders",
        "join_active_customers",
        "explode_items",
        "filter_negative_prices",
        "compute_daily_city_sales",
        "write_daily_summary_csv",
    ]


def test_run_pipeline_for_dates_handles_errors(monkeypatch):
    processed = []
    errors = []

    class DummySpark:
        def stop(self):
            processed.append("stopped")

    monkeypatch.setattr(orchestrator, "create_spark_session", lambda: DummySpark())
    monkeypatch.setattr(orchestrator, "read_customers", lambda spark: "customers")
    monkeypatch.setattr(orchestrator, "read_refunds", lambda spark: "refunds")

    def fake_process_date(spark, customers, refunds, date):
        processed.append(date)
        if date == "bad":
            raise ValueError("boom")

    monkeypatch.setattr(orchestrator, "process_date", fake_process_date)
    monkeypatch.setattr(orchestrator, "move_to_done", lambda date: processed.append(f"done:{date}"))
    monkeypatch.setattr(orchestrator, "move_to_error", lambda date: errors.append(date))

    orchestrator.run_pipeline_for_dates(["good", "bad"])

    assert processed == ["good", "done:good", "bad", "stopped"]
    assert errors == ["bad"]


def test_run_invokes_pipeline(monkeypatch):
    monkeypatch.setattr(orchestrator, "list_available_dates", lambda: ["2024-01-09"])
    called = []
    monkeypatch.setattr(orchestrator, "run_pipeline_for_dates", lambda dates: called.extend(dates))

    orchestrator.run()

    assert called == ["2024-01-09"]
