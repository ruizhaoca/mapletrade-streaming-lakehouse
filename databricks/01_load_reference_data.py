# Databricks notebook source
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

UTC = timezone.utc

dbutils.widgets.text("catalog", "mapletrade_dev")
dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()))
catalog = dbutils.widgets.get("catalog")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Unsafe catalog identifier: {catalog!r}")

volume_root = f"/Volumes/{catalog}/reference/raw_files"
dbutils.widgets.text("instrument_path", f"{volume_root}/instruments.csv")
dbutils.widgets.text("portfolio_path", f"{volume_root}/portfolios.csv")
dbutils.widgets.text("fx_path", f"{volume_root}/fx_rates.csv")

manifest_schema = StructType(
    [
        StructField("source_name", StringType()),
        StructField("source_type", StringType()),
        StructField("source_url", StringType()),
        StructField("retrieved_at", TimestampType()),
        StructField("business_date", DateType()),
        StructField("file_path", StringType()),
        StructField("file_format", StringType()),
        StructField("row_count", LongType()),
        StructField("load_status", StringType()),
        StructField("pipeline_run_id", StringType()),
        StructField("error_message", StringType()),
    ]
)


def read_csv(path: str):
    return spark.read.option("header", True).option("mode", "FAILFAST").csv(path)


def write_manifest(row: Row) -> None:
    spark.createDataFrame([row], manifest_schema).write.mode("append").saveAsTable(
        f"{catalog}.reference.ingestion_manifest"
    )


def load_dataset(name: str, path: str, source_url: str, transform) -> None:
    started_at = datetime.now(UTC).replace(tzinfo=None)
    try:
        frame = transform(read_csv(path)).cache()
        row_count = frame.count()
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"{catalog}.reference.{name}"
        )
        business_date = None
        if name == "fx_rates":
            business_date = frame.agg(F.max("rate_date")).first()[0]
        write_manifest(
            Row(
                source_name=name,
                source_type="PUBLIC_API" if name == "fx_rates" else "REFERENCE_FILE",
                source_url=source_url,
                retrieved_at=started_at,
                business_date=business_date,
                file_path=path,
                file_format="CSV",
                row_count=row_count,
                load_status="SUCCESS",
                pipeline_run_id=pipeline_run_id,
                error_message=None,
            )
        )
        frame.unpersist()
        print(f"Loaded {row_count:,} rows into {catalog}.reference.{name}")
    except Exception as exc:
        write_manifest(
            Row(
                source_name=name,
                source_type="PUBLIC_API" if name == "fx_rates" else "REFERENCE_FILE",
                source_url=source_url,
                retrieved_at=started_at,
                business_date=None,
                file_path=path,
                file_format="CSV",
                row_count=0,
                load_status="FAILED",
                pipeline_run_id=pipeline_run_id,
                error_message=str(exc)[:2000],
            )
        )
        raise


load_dataset(
    "instruments",
    dbutils.widgets.get("instrument_path"),
    "https://www.tsx.com/en/listings/listing-with-us/listed-company-directory",
    lambda frame: frame.select(
        "instrument_id",
        "symbol",
        "issuer_name",
        "exchange",
        "asset_class",
        "quote_currency",
        F.col("active_flag").cast("boolean"),
        F.col("effective_date").cast("date"),
        "source",
        "source_url",
        F.col("retrieved_at").cast("timestamp"),
        F.col("quote_currency_is_synthetic").cast("boolean"),
    ),
)
load_dataset(
    "portfolios",
    dbutils.widgets.get("portfolio_path"),
    "SYNTHETIC",
    lambda frame: frame.select(
        "portfolio_id",
        "portfolio_name",
        "strategy",
        "base_currency",
        F.col("active_flag").cast("boolean"),
        F.col("effective_date").cast("date"),
        "source",
    ),
)
load_dataset(
    "fx_rates",
    dbutils.widgets.get("fx_path"),
    "https://www.bankofcanada.ca/valet-api-how-to/",
    lambda frame: frame.select(
        "currency",
        F.col("rate_date").cast("date"),
        F.col("cad_rate").cast("decimal(18,6)"),
        F.col("original_rate_date").cast("date"),
        F.col("is_carried_forward").cast("boolean"),
        "source",
        F.col("retrieved_at").cast("timestamp"),
    ),
)
