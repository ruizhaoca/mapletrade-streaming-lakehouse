from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from mapletrade.spark.tables import TableNames

OUTCOME_COLUMNS = (
    "raw_key",
    "raw_value",
    "payload_format",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "ingested_at",
    "pipeline_run_id",
    "event_id",
    "trade_id",
    "event_version",
    "event_type",
    "event_ts",
    "produced_ts",
    "source_system",
    "portfolio_id",
    "instrument_id",
    "side",
    "quantity",
    "price",
    "trade_currency",
    "trade_date",
    "signed_quantity",
    "local_notional",
    "cad_rate",
    "notional_cad",
    "signed_notional_cad",
    "payload_hash",
    "outcome",
    "failure_reason",
    "failed_rules",
    "processed_at",
    "stream_id",
    "batch_id",
)


def classify_batch(
    batch: DataFrame,
    spark: SparkSession,
    names: TableNames,
) -> DataFrame:
    """Give every previously unseen Kafka message exactly one deterministic outcome."""
    existing_messages = spark.table(names.outcomes).select("topic", "partition", "offset")
    new = batch.dropDuplicates(["topic", "partition", "offset"]).join(
        existing_messages, ["topic", "partition", "offset"], "left_anti"
    )

    seen_ids = (
        spark.table(names.outcomes)
        .where(F.col("event_id").isNotNull())
        .select(F.col("event_id"), F.lit(True).alias("event_id_seen"))
        .dropDuplicates(["event_id"])
    )
    current = spark.table(names.current).select(
        "trade_id",
        F.col("current_version").alias("stored_version"),
        F.col("trade_status").alias("stored_status"),
        F.col("payload_hash").alias("stored_payload_hash"),
    )
    new = new.join(seen_ids, "event_id", "left").join(current, "trade_id", "left")

    arrival = Window.partitionBy("trade_id").orderBy("partition", "offset")
    prior_arrival = arrival.rowsBetween(Window.unboundedPreceding, -1)
    event_id_order = Window.partitionBy("event_id").orderBy("topic", "partition", "offset")
    version_payload_order = Window.partitionBy("trade_id", "event_version", "payload_hash").orderBy(
        "topic", "partition", "offset"
    )

    version_stats = (
        new.where(
            F.col("trade_id").isNotNull()
            & F.col("event_version").isNotNull()
            & (F.size("failed_rules") == 0)
        )
        .groupBy("trade_id", "event_version")
        .agg(F.countDistinct("payload_hash").alias("version_payload_count"))
    )
    staged = (
        new.join(version_stats, ["trade_id", "event_version"], "left")
        .withColumn(
            "event_id_rank",
            F.when(F.col("event_id").isNotNull(), F.row_number().over(event_id_order)).otherwise(1),
        )
        .withColumn("version_payload_rank", F.row_number().over(version_payload_order))
    )
    preeligible = (
        (~F.col("parse_failed"))
        & (F.size("failed_rules") == 0)
        & F.col("event_id_seen").isNull()
        & (F.col("event_id_rank") == 1)
        & (F.coalesce(F.col("version_payload_count"), F.lit(1)) == 1)
        & (F.col("version_payload_rank") == 1)
    )
    staged = (
        staged.withColumn(
            "prior_max_version",
            F.max(F.when(preeligible, F.col("event_version"))).over(prior_arrival),
        )
        .withColumn(
            "prior_valid_new",
            F.max(
                F.when(
                    preeligible & (F.col("event_type") == "NEW") & (F.col("event_version") == 1),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).over(prior_arrival),
        )
        .withColumn(
            "prior_cancel_version",
            F.min(
                F.when(preeligible & (F.col("event_type") == "CANCEL"), F.col("event_version"))
            ).over(prior_arrival),
        )
    )

    baseline_version = F.greatest(
        F.coalesce(F.col("stored_version"), F.lit(0)),
        F.coalesce(F.col("prior_max_version"), F.lit(0)),
    )
    lifecycle_valid = (
        F.when(F.col("stored_status") == "CANCELLED", F.lit(False))
        .when(
            F.col("stored_version").isNull(),
            (
                (F.col("event_type") == "NEW")
                & (F.col("event_version") == 1)
                & (F.coalesce(F.col("prior_valid_new"), F.lit(0)) == 0)
            )
            | (
                (F.col("event_type") != "NEW")
                & (F.coalesce(F.col("prior_valid_new"), F.lit(0)) == 1)
            ),
        )
        .otherwise(F.col("event_type") != "NEW")
        & F.col("prior_cancel_version").isNull()
    )

    outcome = (
        F.when(F.col("parse_failed"), F.lit("QUARANTINED"))
        .when(F.col("event_id_seen") == F.lit(True), F.lit("DUPLICATE"))
        .when(F.col("event_id_rank") > 1, F.lit("DUPLICATE"))
        .when(F.size("failed_rules") > 0, F.lit("QUARANTINED"))
        .when(F.col("version_payload_count") > 1, F.lit("VERSION_CONFLICT"))
        .when(F.col("version_payload_rank") > 1, F.lit("DUPLICATE"))
        .when(
            (F.col("event_version") == F.col("stored_version"))
            & (F.col("payload_hash") == F.col("stored_payload_hash")),
            F.lit("DUPLICATE"),
        )
        .when(
            (F.col("event_version") == F.col("stored_version"))
            & (F.col("payload_hash") != F.col("stored_payload_hash")),
            F.lit("VERSION_CONFLICT"),
        )
        .when(F.col("event_version") < baseline_version, F.lit("STALE_VERSION"))
        .when(~lifecycle_valid, F.lit("QUARANTINED"))
        .otherwise(F.lit("APPLIED"))
    )

    reason = (
        F.when(F.col("parse_failed"), F.lit("PAYLOAD_DESERIALIZATION_FAILED"))
        .when(F.col("event_id_seen") == F.lit(True), F.lit("DUPLICATE_EVENT_ID"))
        .when(F.col("event_id_rank") > 1, F.lit("DUPLICATE_EVENT_ID_IN_BATCH"))
        .when(F.size("failed_rules") > 0, F.col("failure_reason"))
        .when(F.col("version_payload_count") > 1, F.lit("CONFLICTING_TRADE_VERSION"))
        .when(F.col("version_payload_rank") > 1, F.lit("REPLAYED_TRADE_VERSION"))
        .when(F.col("event_version") == F.col("stored_version"), F.lit("EXISTING_TRADE_VERSION"))
        .when(F.col("event_version") < baseline_version, F.lit("LOWER_THAN_APPLIED_VERSION"))
        .when(~lifecycle_valid, F.lit("INVALID_LIFECYCLE_TRANSITION"))
        .otherwise(F.lit("ACCEPTED_HIGHER_VERSION"))
    )

    return staged.withColumn("outcome", outcome).withColumn("outcome_reason", reason)


def process_silver_batch(
    batch: DataFrame,
    batch_id: int,
    spark: SparkSession,
    names: TableNames,
    stream_id: str,
) -> None:
    if batch.isEmpty():
        return

    classified = classify_batch(batch, spark, names)
    outcome_rows = (
        classified.withColumn("failure_reason", F.col("outcome_reason"))
        .withColumn(
            "failed_rules",
            F.when(F.size("failed_rules") > 0, F.col("failed_rules")).otherwise(
                F.array(F.col("outcome_reason"))
            ),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("stream_id", F.lit(stream_id))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .select(*OUTCOME_COLUMNS)
    )

    outcomes_target = DeltaTable.forName(spark, names.outcomes)
    (
        outcomes_target.alias("target")
        .merge(
            outcome_rows.alias("source"),
            "target.topic = source.topic AND "
            "target.partition = source.partition AND target.offset = source.offset",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Reading the durable batch outcomes makes a retry safe if the outcome commit succeeds but
    # the current-state MERGE or streaming checkpoint commit fails.
    batch_applied = spark.table(names.outcomes).where(
        (F.col("stream_id") == stream_id)
        & (F.col("batch_id") == batch_id)
        & (F.col("outcome") == "APPLIED")
    )
    trade_batch = Window.partitionBy("trade_id")
    latest = (
        batch_applied.withColumn("batch_first_seen_at", F.min("ingested_at").over(trade_batch))
        .withColumn(
            "current_rank",
            F.row_number().over(
                Window.partitionBy("trade_id").orderBy(
                    F.desc("event_version"), F.desc("event_ts"), F.desc("offset")
                )
            ),
        )
        .where(F.col("current_rank") == 1)
        .drop("current_rank")
        .withColumn(
            "trade_status",
            F.when(F.col("event_type") == "CANCEL", F.lit("CANCELLED")).otherwise(F.lit("BOOKED")),
        )
        .withColumnRenamed("event_version", "current_version")
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("first_seen_at", F.col("batch_first_seen_at"))
        .withColumn("last_event_ts", F.col("event_ts"))
    )
    if latest.isEmpty():
        return

    current_columns = (
        "trade_id",
        "current_version",
        "trade_status",
        "event_type",
        "event_id",
        "event_ts",
        "portfolio_id",
        "instrument_id",
        "side",
        "quantity",
        "price",
        "trade_currency",
        "trade_date",
        "signed_quantity",
        "local_notional",
        "cad_rate",
        "notional_cad",
        "signed_notional_cad",
        "payload_hash",
        "first_seen_at",
        "last_event_ts",
        "updated_at",
        "pipeline_run_id",
    )
    source = latest.select(*current_columns)
    current_target = DeltaTable.forName(spark, names.current)
    update_map = {column: f"source.{column}" for column in current_columns if column != "trade_id"}
    update_map["first_seen_at"] = "target.first_seen_at"
    insert_map = {column: f"source.{column}" for column in current_columns}
    (
        current_target.alias("target")
        .merge(source.alias("source"), "target.trade_id = source.trade_id")
        .whenMatchedUpdate(
            condition="source.current_version > target.current_version",
            set=update_map,
        )
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
