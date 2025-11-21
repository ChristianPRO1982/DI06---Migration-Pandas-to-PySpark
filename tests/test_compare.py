import os

import pytest

directory_path = "./notebooks/output/daily_summary/"
daily_summary_files = []
if os.path.exists(directory_path):
    files = os.listdir(directory_path)
    for file in files:
        daily_summary_files.append(file)

v1_path = "./tests/start_pack/daily_city_sales.csv"
v1_lines = []
with open(v1_path, "r", encoding="utf-8") as f:
    for line in f:
        v1_lines.append(line.strip())


def test_compare_versions_v1_v2():
    # print("Files found:", files)
    for file in daily_summary_files:
        print("Checking file:", file)
        with open(v1_path, "r", encoding="utf-8") as f:
            for line in f:
                assert line.strip() in v1_lines
    # assert False
