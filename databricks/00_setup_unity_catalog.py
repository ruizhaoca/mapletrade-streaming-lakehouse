# Databricks notebook source
from __future__ import annotations

import re

dbutils.widgets.text("catalog", "mapletrade_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Unsafe catalog identifier: {catalog!r}")


spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'MapleTrade portfolio project'")
for schema_name, description in {
    "bronze": "Raw, append-only source messages",
    "reference": "Batch reference and ingestion metadata",
    "silver": "Validated event outcomes and authoritative current state",
    "gold": "Business-facing position and traded-notional products",
    "ops": "Pipeline state and reconciliation results",
}.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name} COMMENT '{description}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog}.reference.raw_files "
    "COMMENT 'Uploaded raw and normalized reference files'"
)
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.pipeline_state "
    "COMMENT 'Structured Streaming checkpoints and replay inputs'"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.trade_events (
        raw_key BINARY,
        raw_value BINARY NOT NULL,
        payload_format STRING NOT NULL COMMENT 'CONFLUENT_AVRO or JSON_REPLAY',
        topic STRING NOT NULL,
        partition INT NOT NULL,
        offset BIGINT NOT NULL,
        kafka_timestamp TIMESTAMP,
        ingested_at TIMESTAMP NOT NULL,
        pipeline_run_id STRING NOT NULL
    ) USING DELTA
    COMMENT 'Append-only Kafka or replay messages with source metadata; grain is one message'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.reference.instruments (
        instrument_id STRING,
        symbol STRING,
        issuer_name STRING,
        exchange STRING,
        asset_class STRING,
        quote_currency STRING,
        active_flag BOOLEAN,
        effective_date DATE,
        source STRING,
        source_url STRING,
        retrieved_at TIMESTAMP,
        quote_currency_is_synthetic BOOLEAN
    ) USING DELTA
    COMMENT 'Curated TMX instruments with explicitly identified project-generated enrichment'
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.reference.portfolios (
        portfolio_id STRING,
        portfolio_name STRING,
        strategy STRING,
        base_currency STRING,
        active_flag BOOLEAN,
        effective_date DATE,
        source STRING
    ) USING DELTA
    COMMENT 'Fictional portfolio reference data'
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.reference.fx_rates (
        currency STRING,
        rate_date DATE,
        cad_rate DECIMAL(18,6),
        original_rate_date DATE,
        is_carried_forward BOOLEAN,
        source STRING,
        retrieved_at TIMESTAMP
    ) USING DELTA
    COMMENT 'Daily currency-to-CAD rates normalized from the Bank of Canada Valet API'
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.reference.ingestion_manifest (
        source_name STRING,
        source_type STRING,
        source_url STRING,
        retrieved_at TIMESTAMP,
        business_date DATE,
        file_path STRING,
        file_format STRING,
        row_count BIGINT,
        load_status STRING,
        pipeline_run_id STRING,
        error_message STRING
    ) USING DELTA
    COMMENT 'Audit manifest for batch reference loads'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.trade_event_outcomes (
        raw_key BINARY,
        raw_value BINARY,
        payload_format STRING,
        topic STRING NOT NULL,
        partition INT NOT NULL,
        offset BIGINT NOT NULL,
        kafka_timestamp TIMESTAMP,
        ingested_at TIMESTAMP,
        pipeline_run_id STRING,
        event_id STRING,
        trade_id STRING,
        event_version INT,
        event_type STRING,
        event_ts TIMESTAMP,
        produced_ts TIMESTAMP,
        source_system STRING,
        portfolio_id STRING,
        instrument_id STRING,
        side STRING,
        quantity BIGINT,
        price DECIMAL(18,4),
        trade_currency STRING,
        trade_date DATE,
        signed_quantity BIGINT,
        local_notional DECIMAL(28,4),
        cad_rate DECIMAL(18,6),
        notional_cad DECIMAL(28,4),
        signed_notional_cad DECIMAL(28,4),
        payload_hash STRING,
        outcome STRING NOT NULL,
        failure_reason STRING,
        failed_rules ARRAY<STRING>,
        processed_at TIMESTAMP NOT NULL,
        stream_id STRING NOT NULL,
        batch_id BIGINT NOT NULL
    ) USING DELTA
    COMMENT 'One durable processing outcome for every Bronze message'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.trade_current (
        trade_id STRING NOT NULL,
        current_version INT NOT NULL,
        trade_status STRING NOT NULL,
        event_type STRING NOT NULL,
        event_id STRING NOT NULL,
        event_ts TIMESTAMP NOT NULL,
        portfolio_id STRING,
        instrument_id STRING,
        side STRING,
        quantity BIGINT,
        price DECIMAL(18,4),
        trade_currency STRING,
        trade_date DATE,
        signed_quantity BIGINT,
        local_notional DECIMAL(28,4),
        cad_rate DECIMAL(18,6),
        notional_cad DECIMAL(28,4),
        signed_notional_cad DECIMAL(28,4),
        payload_hash STRING,
        first_seen_at TIMESTAMP,
        last_event_ts TIMESTAMP,
        updated_at TIMESTAMP,
        pipeline_run_id STRING
    ) USING DELTA
    COMMENT 'Authoritative BOOKED or CANCELLED state; grain is one trade_id'
    """
)

for view_name, outcome in {
    "vw_valid_events": "APPLIED",
    "vw_quarantined_events": "QUARANTINED",
    "vw_duplicate_events": "DUPLICATE",
    "vw_stale_events": "STALE_VERSION",
    "vw_version_conflicts": "VERSION_CONFLICT",
}.items():
    spark.sql(
        f"CREATE OR REPLACE VIEW {catalog}.silver.{view_name} AS "
        f"SELECT * FROM {catalog}.silver.trade_event_outcomes WHERE outcome = '{outcome}'"
    )

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_reconciliation (
        pipeline_run_id STRING NOT NULL,
        check_name STRING NOT NULL,
        source_value DECIMAL(38,6) NOT NULL,
        target_value DECIMAL(38,6) NOT NULL,
        difference DECIMAL(38,6) NOT NULL,
        tolerance DECIMAL(38,6) NOT NULL,
        status STRING NOT NULL,
        details STRING,
        checked_at TIMESTAMP NOT NULL
    ) USING DELTA
    COMMENT 'Source-to-target correctness checks recorded for each workflow run'
    """
)

print(f"Unity Catalog objects are ready under {catalog}")
