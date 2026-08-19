# Databricks notebook source
from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def find_repo_root() -> Path:
    candidates = [Path.cwd(), *Path.cwd().parents]
    for candidate in candidates:
        if (candidate / "src" / "mapletrade").exists():
            return candidate
    raise RuntimeError("Run this notebook from a Databricks Git folder containing the repository")


repo_root = find_repo_root()
sys.path.insert(0, str(repo_root / "src"))

from mapletrade.spark.lifecycle import process_silver_batch  # noqa: E402
from mapletrade.spark.tables import TableNames  # noqa: E402
from mapletrade.spark.transformations import (  # noqa: E402
    load_avro_schema,
    parse_and_enrich_trade_events,
)

dbutils.widgets.text("catalog", "mapletrade_dev")
dbutils.widgets.dropdown("source_mode", "kafka", ["kafka", "replay"])
dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()))
dbutils.widgets.text("secret_scope", "mapletrade")
dbutils.widgets.text("bootstrap_servers", "")
dbutils.widgets.text("kafka_topic", "mapletrade.trade-events.v1")
catalog = dbutils.widgets.get("catalog")
source_mode = dbutils.widgets.get("source_mode")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Unsafe catalog identifier: {catalog!r}")

names = TableNames(catalog)
dbutils.widgets.text("replay_path", f"{names.volume_root}/replay")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")


if source_mode == "kafka":
    bootstrap_servers = dbutils.widgets.get("bootstrap_servers")
    if not bootstrap_servers:
        raise ValueError("bootstrap_servers is required in kafka mode")
    scope = dbutils.widgets.get("secret_scope")
    kafka_key = dbutils.secrets.get(scope=scope, key="kafka-api-key")
    kafka_secret = dbutils.secrets.get(scope=scope, key="kafka-api-secret")
    jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_key}" password="{kafka_secret}";'
    )
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", dbutils.widgets.get("kafka_topic"))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .load()
    )
    bronze_rows = source.select(
        F.col("key").alias("raw_key"),
        F.col("value").alias("raw_value"),
        F.lit("CONFLUENT_AVRO").alias("payload_format"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
    )
    bronze_checkpoint = f"{names.volume_root}/checkpoints/kafka_to_bronze"
else:
    replay_schema = StructType(
        [
            StructField("replay_offset", LongType()),
            StructField("event_id", StringType()),
            StructField("trade_id", StringType()),
            StructField("produced_ts", LongType()),
        ]
    )
    source = (
        spark.readStream.format("text")
        .option("pathGlobFilter", "*.jsonl")
        .load(dbutils.widgets.get("replay_path"))
    )
    envelope = source.withColumn("envelope", F.from_json("value", replay_schema))
    bronze_rows = envelope.select(
        F.col("envelope.trade_id").cast("binary").alias("raw_key"),
        F.col("value").cast("binary").alias("raw_value"),
        F.lit("JSON_REPLAY").alias("payload_format"),
        F.lit("mapletrade.replay.v1").alias("topic"),
        F.lit(0).cast("int").alias("partition"),
        F.col("envelope.replay_offset").cast("long").alias("offset"),
        (F.col("envelope.produced_ts") / F.lit(1000)).cast("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
    )
    bronze_checkpoint = f"{names.volume_root}/checkpoints/replay_to_bronze"

bronze_query = (
    bronze_rows.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .toTable(names.bronze_events)
)
bronze_query.awaitTermination()

avro_schema = load_avro_schema(repo_root / "schemas" / "trade_event_v1.avsc")
bronze_stream = spark.readStream.table(names.bronze_events)
enriched = parse_and_enrich_trade_events(bronze_stream, spark, names, avro_schema)
silver_query = (
    enriched.writeStream.foreachBatch(
        lambda frame, batch_id: process_silver_batch(
            frame,
            batch_id,
            spark=spark,
            names=names,
            stream_id="bronze-to-silver-v1",
        )
    )
    .option("checkpointLocation", f"{names.volume_root}/checkpoints/bronze_to_silver")
    .trigger(availableNow=True)
    .start()
)
silver_query.awaitTermination()

print(f"Completed ingestion and transformation run {pipeline_run_id} in {source_mode} mode")
