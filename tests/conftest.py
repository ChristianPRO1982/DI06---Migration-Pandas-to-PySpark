import importlib.util

import pytest

if importlib.util.find_spec("pyspark") is None:
    pytest.skip("pyspark is required for pipeline tests", allow_module_level=True)

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pipeline-tests")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
