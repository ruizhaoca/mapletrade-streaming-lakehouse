from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from mapletrade.spark.gold import currency_notional_frame, position_snapshot_frame  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    try:
        session = SparkSession.builder.master("local[2]").appName("mapletrade-tests").getOrCreate()
    except Exception as exc:
        pytest.skip(f"Local Spark runtime unavailable: {exc}")
    yield session
    session.stop()


@pytest.mark.spark
def test_gold_aggregations(spark) -> None:
    rows = [
        (
            "T1",
            "P1",
            "I1",
            "BUY",
            100,
            100,
            Decimal("1000.0000"),
            Decimal("1000.0000"),
            Decimal("1000.0000"),
            "CAD",
        ),
        (
            "T2",
            "P1",
            "I1",
            "SELL",
            40,
            -40,
            Decimal("400.0000"),
            Decimal("400.0000"),
            Decimal("-400.0000"),
            "CAD",
        ),
    ]
    booked = spark.createDataFrame(
        rows,
        "trade_id string, portfolio_id string, instrument_id string, side string, "
        "quantity long, signed_quantity long, local_notional decimal(28,4), "
        "notional_cad decimal(28,4), signed_notional_cad decimal(28,4), trade_currency string",
    )
    position = position_snapshot_frame(booked, "2026-08-11").first()
    currency = currency_notional_frame(booked, "2026-08-11").first()

    assert position.as_of_date == date(2026, 8, 11)
    assert position.net_quantity == 60
    assert position.active_trade_count == 2
    assert currency.net_signed_notional_cad == Decimal("600.0000")
