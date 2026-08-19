from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from mapletrade.spark.tables import TableNames


def load_avro_schema(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def parse_and_enrich_trade_events(
    bronze: DataFrame,
    spark: SparkSession,
    names: TableNames,
    avro_schema: str,
) -> DataFrame:
    """Deserialize Confluent-wire-format Avro and apply static reference joins."""
    # Byte 1 is Confluent's magic byte and bytes 2-5 contain the schema ID.
    avro_payload = F.expr("substring(raw_value, 6, length(raw_value) - 5)")
    json_schema = StructType(
        [
            StructField("replay_offset", LongType()),
            StructField("event_id", StringType()),
            StructField("trade_id", StringType()),
            StructField("event_version", IntegerType()),
            StructField("event_type", StringType()),
            StructField("event_ts", LongType()),
            StructField("produced_ts", LongType()),
            StructField("source_system", StringType()),
            StructField("portfolio_id", StringType()),
            StructField("instrument_id", StringType()),
            StructField("side", StringType()),
            StructField("quantity", LongType()),
            StructField("price", StringType()),
            StructField("trade_currency", StringType()),
        ]
    )
    parsed = bronze.withColumn(
        "avro_decoded",
        F.when(
            F.col("payload_format") == "CONFLUENT_AVRO",
            from_avro(avro_payload, avro_schema, {"mode": "PERMISSIVE"}),
        ),
    ).withColumn(
        "json_decoded",
        F.when(
            F.col("payload_format") == "JSON_REPLAY",
            F.from_json(F.col("raw_value").cast("string"), json_schema),
        ),
    )
    is_json = F.col("payload_format") == "JSON_REPLAY"

    def decoded(field: str):
        return F.when(is_json, F.col(f"json_decoded.{field}")).otherwise(
            F.col(f"avro_decoded.{field}")
        )

    event = parsed.select(
        "raw_key",
        "raw_value",
        "payload_format",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "ingested_at",
        "pipeline_run_id",
        decoded("event_id").alias("event_id"),
        decoded("trade_id").alias("trade_id"),
        decoded("event_version").alias("event_version"),
        decoded("event_type").cast("string").alias("event_type"),
        F.when(
            is_json,
            (F.col("json_decoded.event_ts") / F.lit(1000)).cast("timestamp"),
        )
        .otherwise(F.col("avro_decoded.event_ts"))
        .alias("event_ts"),
        F.when(
            is_json,
            (F.col("json_decoded.produced_ts") / F.lit(1000)).cast("timestamp"),
        )
        .otherwise(F.col("avro_decoded.produced_ts"))
        .alias("produced_ts"),
        decoded("source_system").alias("source_system"),
        decoded("portfolio_id").alias("portfolio_id"),
        decoded("instrument_id").alias("instrument_id"),
        decoded("side").cast("string").alias("side"),
        decoded("quantity").alias("quantity"),
        decoded("price").cast(DecimalType(18, 4)).alias("price"),
        decoded("trade_currency").alias("trade_currency"),
        F.when(is_json, F.col("json_decoded").isNull())
        .otherwise(F.col("avro_decoded").isNull())
        .alias("parse_failed"),
    ).withColumn("trade_date", F.to_date("event_ts"))

    instruments = spark.table(names.instruments).select(
        "instrument_id",
        F.col("active_flag").alias("instrument_active"),
        F.col("quote_currency").alias("instrument_currency"),
    )
    portfolios = spark.table(names.portfolios).select(
        "portfolio_id",
        F.col("active_flag").alias("portfolio_active"),
    )
    fx = spark.table(names.fx_rates).select(
        "currency",
        "rate_date",
        "cad_rate",
        "original_rate_date",
        "is_carried_forward",
    )

    enriched = (
        event.join(F.broadcast(instruments), "instrument_id", "left")
        .join(F.broadcast(portfolios), "portfolio_id", "left")
        .join(
            F.broadcast(fx),
            (event.trade_currency == fx.currency) & (event.trade_date == fx.rate_date),
            "left",
        )
        .drop("currency", "rate_date")
    )

    failures = F.filter(
        F.array(
            F.when(F.col("parse_failed"), F.lit("PAYLOAD_DESERIALIZATION_FAILED")),
            F.when(F.col("event_id").isNull(), F.lit("MISSING_EVENT_ID")),
            F.when(F.col("trade_id").isNull(), F.lit("MISSING_TRADE_ID")),
            F.when(
                F.col("event_version").isNull() | (F.col("event_version") <= 0),
                F.lit("INVALID_EVENT_VERSION"),
            ),
            F.when(
                F.col("event_type").isNull() | ~F.col("event_type").isin("NEW", "AMEND", "CANCEL"),
                F.lit("INVALID_EVENT_TYPE"),
            ),
            F.when(F.col("event_ts").isNull(), F.lit("MISSING_EVENT_TIMESTAMP")),
            F.when(
                F.col("side").isNull() | ~F.col("side").isin("BUY", "SELL"),
                F.lit("INVALID_SIDE"),
            ),
            F.when(
                F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("INVALID_QUANTITY")
            ),
            F.when(F.col("price").isNull() | (F.col("price") <= 0), F.lit("INVALID_PRICE")),
            F.when(F.col("instrument_active").isNull(), F.lit("UNKNOWN_INSTRUMENT")),
            F.when(F.col("instrument_active") == F.lit(False), F.lit("INACTIVE_INSTRUMENT")),
            F.when(F.col("portfolio_active").isNull(), F.lit("UNKNOWN_PORTFOLIO")),
            F.when(F.col("portfolio_active") == F.lit(False), F.lit("INACTIVE_PORTFOLIO")),
            F.when(F.col("cad_rate").isNull(), F.lit("MISSING_FX_RATE")),
            F.when(
                F.col("instrument_currency").isNotNull()
                & (F.col("instrument_currency") != F.col("trade_currency")),
                F.lit("TRADE_CURRENCY_MISMATCH"),
            ),
        ),
        lambda rule: rule.isNotNull(),
    )

    payload_columns = [
        "trade_id",
        "event_version",
        "event_type",
        "event_ts",
        "portfolio_id",
        "instrument_id",
        "side",
        "quantity",
        "price",
        "trade_currency",
    ]
    signed_quantity = F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(
        -F.col("quantity")
    )
    return (
        enriched.withColumn("failed_rules", failures)
        .withColumn("failure_reason", F.concat_ws("|", failures))
        .withColumn(
            "payload_hash",
            F.sha2(F.to_json(F.struct(*[F.col(column) for column in payload_columns])), 256),
        )
        .withColumn("signed_quantity", signed_quantity.cast("long"))
        .withColumn("local_notional", (F.col("quantity") * F.col("price")).cast("decimal(28,4)"))
        .withColumn(
            "notional_cad",
            (F.col("quantity") * F.col("price") * F.col("cad_rate")).cast("decimal(28,4)"),
        )
        .withColumn(
            "signed_notional_cad",
            (F.col("signed_quantity") * F.col("price") * F.col("cad_rate")).cast("decimal(28,4)"),
        )
    )
