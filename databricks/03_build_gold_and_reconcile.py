# Databricks notebook source
from __future__ import annotations

import re
import sys
import uuid
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F


def find_repo_root() -> Path:
    candidates = [Path.cwd(), *Path.cwd().parents]
    for candidate in candidates:
        if (candidate / "src" / "mapletrade").exists():
            return candidate
    raise RuntimeError("Run this notebook from a Databricks Git folder containing the repository")


repo_root = find_repo_root()
sys.path.insert(0, str(repo_root / "src"))

from mapletrade.spark.gold import build_gold_tables  # noqa: E402
from mapletrade.spark.reconciliation import run_reconciliation  # noqa: E402
from mapletrade.spark.tables import TableNames  # noqa: E402

dbutils.widgets.text("catalog", "mapletrade_dev")
dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()))
dbutils.widgets.text("as_of_date", date.today().isoformat())
catalog = dbutils.widgets.get("catalog")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
as_of_date = dbutils.widgets.get("as_of_date")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Unsafe catalog identifier: {catalog!r}")
date.fromisoformat(as_of_date)

names = TableNames(catalog)
build_gold_tables(spark, names, as_of_date)
results = run_reconciliation(spark, names, pipeline_run_id, as_of_date)
display(results.orderBy("check_name"))

failed_checks = results.where(F.col("status") == "FAIL").count()
if failed_checks:
    raise RuntimeError(f"{failed_checks} reconciliation check(s) failed")
print(f"Gold build and reconciliation passed for run {pipeline_run_id}")
