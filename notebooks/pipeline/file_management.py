# This module handles movement of input files to 'done' or 'error'
# directories, ensuring that each file is tracked independently.
# It allows the pipeline to continue even if a specific file fails.


import shutil
from pathlib import Path

from .config import DONE_DIR, ERROR_DIR, INPUT_DIR, ORDERS_EXTENSION, ORDERS_PREFIX


def build_input_file_path(date_str: str) -> Path:
    """
    Build the path to the orders JSON file for a given date (YYYY-MM-DD).
    """
    filename = f"{ORDERS_PREFIX}{date_str}{ORDERS_EXTENSION}"
    return INPUT_DIR / filename


def move_to_done(date_str: str) -> Path:
    """
    Move the corresponding JSON file to the 'done' folder.
    """
    src = build_input_file_path(date_str)
    dst = DONE_DIR / src.name
    DONE_DIR.mkdir(exist_ok=True, parents=True)
    shutil.move(str(src), dst)
    return dst


def move_to_error(date_str: str) -> Path:
    """
    Move the corresponding JSON file to the 'error' folder.
    """
    src = build_input_file_path(date_str)
    dst = ERROR_DIR / src.name
    ERROR_DIR.mkdir(exist_ok=True, parents=True)
    shutil.move(str(src), dst)
    return dst


def move_path_to_error(path: Path) -> Path:
    """
    Move an arbitrary path to the 'error' folder.
    """
    ERROR_DIR.mkdir(exist_ok=True, parents=True)
    dst = ERROR_DIR / path.name
    shutil.move(str(path), dst)
    return dst
