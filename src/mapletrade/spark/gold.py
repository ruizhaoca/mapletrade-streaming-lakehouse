from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mapletrade.spark.tables import TableNames


def position_snapshot_frame(booked: DataFrame, as_of_date: str) -> DataFrame:
    return (
        booked.groupBy("portfolio_id", "instrument_id")
        .agg(
            F.sum("signed_quantity").cast("long").alias("net_quantity"),
            F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(0))
            .cast("long")
            .alias("gross_buy_quantity"),
            F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0))
            .cast("long")
            .alias("gross_sell_quantity"),
            F.sum("local_notional").cast("decimal(38,4)").alias("gross_local_notional"),
            F.sum("signed_notional_cad").cast("decimal(38,4)").alias("net_signed_notional_cad"),
            F.count("trade_id").cast("long").alias("active_trade_count"),
        )
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "as_of_date",
            "portfolio_id",
            "instrument_id",
            "net_quantity",
            "gross_buy_quantity",
            "gross_sell_quantity",
            "gross_local_notional",
            "net_signed_notional_cad",
            "active_trade_count",
            "refreshed_at",
        )
    )


def currency_notional_frame(booked: DataFrame, as_of_date: str) -> DataFrame:
    return (
        booked.groupBy("portfolio_id", "trade_currency")
        .agg(
            F.count("trade_id").cast("long").alias("active_trade_count"),
            F.sum("local_notional").cast("decimal(38,4)").alias("gross_local_notional"),
            F.sum("notional_cad").cast("decimal(38,4)").alias("gross_notional_cad"),
            F.sum("signed_notional_cad").cast("decimal(38,4)").alias("net_signed_notional_cad"),
        )
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "as_of_date",
            "portfolio_id",
            "trade_currency",
            "active_trade_count",
            "gross_local_notional",
            "gross_notional_cad",
            "net_signed_notional_cad",
            "refreshed_at",
        )
    )


def build_gold_tables(spark: SparkSession, names: TableNames, as_of_date: str) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {names.booked_view}
        COMMENT 'Logical view of valid BOOKED trades; no duplicate active-trade table'
        AS
        SELECT *
        FROM {names.current}
        WHERE trade_status = 'BOOKED'
        """
    )
    booked = spark.table(names.booked_view)
    (
        position_snapshot_frame(booked, as_of_date)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(names.positions)
    )
    (
        currency_notional_frame(booked, as_of_date)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(names.currency_notional)
    )
