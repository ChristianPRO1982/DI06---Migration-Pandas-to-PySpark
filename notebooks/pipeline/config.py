# This module centralizes all pipeline configuration:
# directory paths, file patterns and date formats.
# All other modules import settings from here.

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = BASE_DIR / "data" / "input"
STATIC_DIR = BASE_DIR / "data" / "statics"
DONE_DIR = BASE_DIR / "data" / "done"
ERROR_DIR = BASE_DIR / "data" / "error"
OUTPUT_DIR = BASE_DIR / "output" / "daily_summary"

ORDERS_PREFIX = "orders_"
ORDERS_EXTENSION = ".json"

DATE_INPUT_FORMAT = "%Y-%m-%d"
DATE_OUTPUT_FORMAT = "%Y%m%d"
