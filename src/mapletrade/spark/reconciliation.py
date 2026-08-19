from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mapletrade.spark.gold import currency_notional_frame, position_snapshot_frame
from mapletrade.spark.tables import TableNames

UTC = timezone.utc

RESULT_SCHEMA = StructType(
    [
        StructField("pipeline_run_id", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("source_value", DecimalType(38, 6), False),
        StructField("target_value", DecimalType(38, 6), False),
        StructField("difference", DecimalType(38, 6), False),
        StructField("tolerance", DecimalType(38, 6), False),
        StructField("status", StringType(), False),
        StructField("details", StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ]
)


def _count(frame: DataFrame) -> int:
    return frame.count()


def _result(
    pipeline_run_id: str,
    check_name: str,
    source_value: int | Decimal,
    target_value: int | Decimal,
    tolerance: Decimal = Decimal("0"),
    *,
    mismatch_count: int | None = None,
    details: dict[str, object] | None = None,
) -> Row:
    source = Decimal(str(source_value))
    target = Decimal(str(target_value))
    difference = abs(source - target)
    passed = difference <= tolerance and (mismatch_count in (None, 0))
    detail_payload = dict(details or {})
    if mismatch_count is not None:
        detail_payload["mismatch_count"] = mismatch_count
    return Row(
        pipeline_run_id=pipeline_run_id,
        check_name=check_name,
        source_value=source,
        target_value=target,
        difference=difference,
        tolerance=tolerance,
        status="PASS" if passed else "FAIL",
        details=json.dumps(detail_payload, sort_keys=True) if detail_payload else None,
        checked_at=datetime.now(UTC).replace(tzinfo=None),
    )


def compare_aggregates(
    expected: DataFrame,
    actual: DataFrame,
    keys: Iterable[str],
    exact_metrics: Iterable[str],
    decimal_metrics: Iterable[str],
    decimal_tolerance: Decimal = Decimal("0.01"),
) -> tuple[int, int, int]:
    keys = tuple(keys)
    exact_metrics = tuple(exact_metrics)
    decimal_metrics = tuple(decimal_metrics)
    expected_count = expected.count()
    actual_count = actual.count()
    joined = (
        expected.withColumn("_expected_present", F.lit(1))
        .alias("expected")
        .join(actual.withColumn("_actual_present", F.lit(1)).alias("actual"), list(keys), "full")
    )

    missing_side = (
        F.col("expected._expected_present").isNull() | F.col("actual._actual_present").isNull()
    )

    metric_mismatch = F.lit(False)
    for metric in exact_metrics:
        metric_mismatch = metric_mismatch | (
            F.coalesce(F.col(f"expected.{metric}"), F.lit(0))
            != F.coalesce(F.col(f"actual.{metric}"), F.lit(0))
        )
    for metric in decimal_metrics:
        metric_mismatch = metric_mismatch | (
            F.abs(
                F.coalesce(F.col(f"expected.{metric}"), F.lit(0))
                - F.coalesce(F.col(f"actual.{metric}"), F.lit(0))
            )
            > F.lit(decimal_tolerance)
        )
    mismatch_count = joined.where(missing_side | metric_mismatch).count()
    return expected_count, actual_count, mismatch_count


def run_reconciliation(
    spark: SparkSession,
    names: TableNames,
    pipeline_run_id: str,
    as_of_date: str,
) -> DataFrame:
    bronze = spark.table(names.bronze_events)
    outcomes = spark.table(names.outcomes)
    current = spark.table(names.current)
    results: list[Row] = []

    bronze_count = bronze.count()
    outcome_count = outcomes.count()
    results.append(_result(pipeline_run_id, "bronze_vs_outcome_count", bronze_count, outcome_count))
    duplicated_bronze = (
        bronze.groupBy("topic", "partition", "offset").count().where(F.col("count") != 1).count()
    )
    results.append(_result(pipeline_run_id, "duplicate_bronze_keys", 0, duplicated_bronze))
    missing_outcomes = bronze.join(outcomes, ["topic", "partition", "offset"], "left_anti").count()
    results.append(_result(pipeline_run_id, "missing_outcome_keys", 0, missing_outcomes))
    orphan_outcomes = outcomes.join(bronze, ["topic", "partition", "offset"], "left_anti").count()
    results.append(_result(pipeline_run_id, "orphan_outcome_keys", 0, orphan_outcomes))
    duplicated_outcomes = (
        outcomes.groupBy("topic", "partition", "offset").count().where(F.col("count") != 1).count()
    )
    results.append(_result(pipeline_run_id, "duplicate_outcome_keys", 0, duplicated_outcomes))

    expected_current = (
        outcomes.where(F.col("outcome") == "APPLIED")
        .withColumn(
            "latest_rank",
            F.row_number().over(
                Window.partitionBy("trade_id").orderBy(
                    F.desc("event_version"), F.desc("event_ts"), F.desc("offset")
                )
            ),
        )
        .where(F.col("latest_rank") == 1)
        .select(
            "trade_id",
            F.col("event_version").alias("expected_version"),
            F.when(F.col("event_type") == "CANCEL", F.lit("CANCELLED"))
            .otherwise(F.lit("BOOKED"))
            .alias("expected_status"),
            F.col("payload_hash").alias("expected_payload_hash"),
        )
    )
    current_comparison = expected_current.alias("expected").join(
        current.select(
            "trade_id",
            "current_version",
            "trade_status",
            "payload_hash",
        ).alias("actual"),
        "trade_id",
        "full",
    )
    current_mismatches = current_comparison.where(
        F.col("expected.expected_version").isNull()
        | F.col("actual.current_version").isNull()
        | (F.col("expected.expected_version") != F.col("actual.current_version"))
        | (F.col("expected.expected_status") != F.col("actual.trade_status"))
        | (F.col("expected.expected_payload_hash") != F.col("actual.payload_hash"))
    ).count()
    results.append(
        _result(
            pipeline_run_id,
            "highest_version_current_state",
            expected_current.count(),
            current.count(),
            mismatch_count=current_mismatches,
        )
    )

    booked = current.where(F.col("trade_status") == "BOOKED")
    expected_positions = position_snapshot_frame(booked, as_of_date).drop("refreshed_at")
    actual_positions = spark.table(names.positions).drop("refreshed_at")
    expected_count, actual_count, mismatch_count = compare_aggregates(
        expected_positions,
        actual_positions,
        ("as_of_date", "portfolio_id", "instrument_id"),
        ("net_quantity", "gross_buy_quantity", "gross_sell_quantity", "active_trade_count"),
        ("gross_local_notional", "net_signed_notional_cad"),
    )
    results.append(
        _result(
            pipeline_run_id,
            "position_snapshot_matches_current",
            expected_count,
            actual_count,
            mismatch_count=mismatch_count,
        )
    )

    expected_notional = currency_notional_frame(booked, as_of_date).drop("refreshed_at")
    actual_notional = spark.table(names.currency_notional).drop("refreshed_at")
    expected_count, actual_count, mismatch_count = compare_aggregates(
        expected_notional,
        actual_notional,
        ("as_of_date", "portfolio_id", "trade_currency"),
        ("active_trade_count",),
        ("gross_local_notional", "gross_notional_cad", "net_signed_notional_cad"),
    )
    results.append(
        _result(
            pipeline_run_id,
            "currency_notional_matches_current",
            expected_count,
            actual_count,
            mismatch_count=mismatch_count,
        )
    )

    result_frame = spark.createDataFrame(results, schema=RESULT_SCHEMA)
    result_frame.write.mode("append").saveAsTable(names.reconciliation)
    return result_frame
