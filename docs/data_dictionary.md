# Data dictionary

All objects use the `mapletrade_dev` catalog by default.

## Bronze

### `bronze.trade_events`

Grain: one Kafka or replay message.

| Important column | Meaning |
|---|---|
| `raw_key`, `raw_value` | Untouched source key and payload bytes |
| `payload_format` | `CONFLUENT_AVRO` or disclosed `JSON_REPLAY` |
| `topic`, `partition`, `offset` | Unique source-message identity |
| `kafka_timestamp` | Kafka timestamp or synthetic replay production timestamp |
| `ingested_at` | Databricks ingestion timestamp |
| `pipeline_run_id` | Workflow run responsible for ingestion |

## Reference

- `reference.instruments`: curated issuer attributes plus clearly marked synthetic enrichment.
- `reference.portfolios`: five fictional active portfolios.
- `reference.fx_rates`: one row per calendar date/currency with the original publication date and
  carry-forward flag.
- `reference.ingestion_manifest`: source, file, count, status, and error metadata for each load.

## Silver

### `silver.trade_event_outcomes`

Grain: one Bronze message. It retains parsed business fields, calculations, message metadata,
payload hash, outcome, and diagnostic reason.

Financial calculations:

```text
signed_quantity = quantity for BUY; -quantity for SELL
local_notional = quantity × price
notional_cad = quantity × price × cad_rate
signed_notional_cad = signed_quantity × price × cad_rate
```

Prices and notionals use `DECIMAL`, not floating point.

### `silver.trade_current`

Grain: one `trade_id`. `current_version` is monotonically increasing and `trade_status` is either
`BOOKED` or `CANCELLED`. It is the only physical current-trade table.

## Gold

### `gold.vw_valid_booked_trade`

A logical view of `silver.trade_current WHERE trade_status = 'BOOKED'`. It deliberately avoids a
duplicate physical active-trade table.

### `gold.position_snapshot`

Grain: `as_of_date + portfolio_id + instrument_id`.

Measures include net quantity, gross buy/sell quantity, gross local notional, net signed CAD
notional, and active trade count.

### `gold.net_traded_notional_by_currency`

Grain: `as_of_date + portfolio_id + trade_currency`.

Measures include active trade count, gross local/CAD notional, and net signed CAD notional. This is
traded-notional reporting, not market-value or risk exposure.

## Operations

### `ops.pipeline_reconciliation`

Grain: one check per pipeline run. It stores the check name, source/target values, difference,
tolerance, pass/fail status, diagnostic JSON, and timestamp.

