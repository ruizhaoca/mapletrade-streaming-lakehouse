# MapleTrade Streaming Lakehouse

MapleTrade is a Databricks lakehouse portfolio project that processes synthetic
`NEW`, `AMEND`, and `CANCEL` trade-booking events from Confluent Kafka. PySpark Structured
Streaming, Spark SQL, and Delta Lake create traceable event outcomes, authoritative current
trade state, portfolio positions, and CAD-denominated traded-notional products.

> **Execution status:** verified end to end in Kafka mode on 2026-08-22. A Databricks Serverless
> Workflow processed 11,676 Confluent Avro messages through Bronze, Silver, Gold, and eight
> reconciliation checks. The first successful run completed in 1 minute 36 seconds; a 49-second
> restart run preserved every business-table count and passed all eight checks again.

## Why this project exists

The project demonstrates more than a happy-path medallion pipeline. It deliberately generates
duplicate, invalid, late, and conflicting events and proves that:

- every ingested message receives exactly one processing outcome;
- a late event cannot overwrite a newer trade version;
- retries do not create duplicate Silver records;
- `CANCEL` produces `CANCELLED` current state and contributes nothing to Gold positions; and
- independently recomputed Silver and Gold results reconcile.

## Architecture

The numbers show execution order. Every table and view shown below is governed in the
`mapletrade_dev` Unity Catalog catalog.

```mermaid
flowchart LR
    subgraph S["0 · Sources"]
        GEN["Synthetic OMS<br/>NEW · AMEND · CANCEL"] --> KAFKA["Confluent Kafka<br/>Avro + Schema Registry"]
        TMX["TMX instruments"] --> REFLOAD["Batch reference load"]
        BOC["Bank of Canada FX"] --> REFLOAD
        PORT["Synthetic portfolios"] --> REFLOAD
        REFLOAD --> REF[("reference tables<br/>+ ingestion_manifest")]
    end

    subgraph T1["Workflow Task 1 · ingest_and_transform"]
        INGEST["1 · Structured Streaming<br/>availableNow + checkpoint"]
        BRONZE[("2 · bronze.trade_events<br/>raw message + source metadata")]
        VALIDATE["3 · Parse, enrich, validate"]
        SEQUENCE["4 · Sequence + idempotency<br/>event_id · event_version · payload_hash"]
        OUTCOMES[("5A · silver.trade_event_outcomes<br/>one outcome per message")]
        MERGE["5B · Version-aware Delta MERGE"]
        CURRENT[("6 · silver.trade_current<br/>one row per trade_id")]
        INGEST --> BRONZE --> VALIDATE --> SEQUENCE
        SEQUENCE --> OUTCOMES
        SEQUENCE --> MERGE --> CURRENT
    end

    subgraph T2["Workflow Task 2 · build_gold_and_reconcile"]
        BOOKED["7 · gold.vw_valid_booked_trade"]
        GOLD["8 · Deterministic Spark SQL build"]
        POSITION[("9A · gold.position_snapshot")]
        NOTIONAL[("9B · gold.net_traded_notional_by_currency")]
        CHECK["10 · Spark SQL reconciliation"]
        RESULTS[("11 · ops.pipeline_reconciliation")]
        BOOKED --> GOLD --> POSITION
        GOLD --> NOTIONAL
        POSITION --> CHECK
        NOTIONAL --> CHECK --> RESULTS
    end

    KAFKA --> INGEST
    REF --> VALIDATE
    CURRENT --> BOOKED
    BRONZE -. message completeness .-> CHECK
    OUTCOMES -. accepted event history .-> CHECK
    CURRENT -. latest state .-> CHECK
```

See [architecture.md](docs/architecture.md) for the processing rules and failure-recovery design.

## Processing outcomes

`silver.trade_event_outcomes` replaces separate physical quarantine, duplicate, and stale tables.
Convenience views expose each category.

| Outcome | Meaning |
|---|---|
| `APPLIED` | Valid lifecycle event accepted into authoritative history |
| `DUPLICATE` | Repeated `event_id` or equivalent replay of the same trade version |
| `QUARANTINED` | Payload, reference, business, or lifecycle validation failed |
| `STALE_VERSION` | A lower version arrived after a newer accepted version |
| `VERSION_CONFLICT` | The same trade/version carried different business content |

## Data sources

- Trade transactions are deterministic synthetic data; no customer or institutional trades are used.
- Instrument names and symbols are curated from the [TMX Listed Company Directory](https://www.tsx.com/en/listings/listing-with-us/listed-company-directory).
- Daily currency-to-CAD rates come from the [Bank of Canada Valet API](https://www.bankofcanada.ca/valet-api-how-to/).
- Portfolio records and instrument enrichment fields are explicitly identified as synthetic.

The committed sample uses 20 instruments, five fictional portfolios, and Bank of Canada rates for
2026-08-11. The default generator creates 10,000 base trades and approximately 11,000–12,000
lifecycle messages after amendments, cancellations, and failure injection.

## Repository structure

```text
mapletrade-streaming-lakehouse/
├── producer/                 # Local generator wrapper and Confluent producer
├── schemas/                  # Avro v1 data contract
├── ingestion/                # TMX, Bank of Canada, and portfolio preparation
├── src/mapletrade/           # Pure Python and reusable PySpark/Delta logic
├── databricks/               # Setup, reference load, two Workflow notebooks, job template
├── scripts/                  # One-command local MVP data preparation
├── tests/unit/               # Generator, quality, lifecycle, schema, and acquisition tests
├── tests/spark/              # Spark aggregation tests
├── data/sample/              # Small attributable reference samples
└── docs/                     # Architecture, dictionary, runbook, decisions, performance
```

## Local quick start

Python 3.10 or later is required. Java is required only for local Spark tests; Databricks Runtime
provides Spark and Delta Lake for the actual pipeline.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[producer,dev]"

# Uses the committed FX sample and produces a ready-to-upload bundle.
python scripts/prepare_mvp_data.py --num-trades 10000 --use-sample-fx

pytest -q
```

To retrieve current public FX data instead of the committed sample:

```powershell
python scripts/prepare_mvp_data.py --num-trades 10000 --event-date 2026-08-11
```

Generated files are written to:

```text
data/upload/reference/   # instruments.csv, portfolios.csv, fx_rates.csv
data/upload/replay/      # trade_events.jsonl and generation metadata
```

## Publish to Confluent Kafka

1. Create topic `mapletrade.trade-events.v1` with three partitions.
2. Create Kafka and Schema Registry API keys.
3. Copy `.env.example` to `.env` and set the values locally.
4. Export those values into the shell; `.env` is intentionally not loaded implicitly.
5. Run:

```powershell
python producer/kafka_producer.py `
  --events data/generated/mvp/trade_events.jsonl `
  --schema schemas/trade_event_v1.avsc
```

The Avro serializer registers the value schema and publishes with `trade_id` as the Kafka key.
Producer idempotence and `acks=all` are enabled, while downstream event idempotency is enforced
independently in Silver.

## Run on Databricks

The detailed checklist is in [runbook.md](docs/runbook.md). In summary:

1. Add this repository to a Databricks Git folder.
2. Run `databricks/00_setup_unity_catalog.py`.
3. Upload the three reference CSVs to
   `/Volumes/mapletrade_dev/reference/raw_files/`.
4. Run `databricks/01_load_reference_data.py`.
5. Add Confluent credentials to the `mapletrade` Databricks secret scope.
6. Replace placeholders in `databricks/workflow.template.json`, create the Workflow, and run it.

If outbound Confluent connectivity is unavailable, upload `trade_events.jsonl` into
`/Volumes/mapletrade_dev/ops/pipeline_state/replay/` and set `source_mode=replay`. The README and
portfolio evidence must state which mode was actually executed.

## Verification queries

```sql
SELECT outcome, count(*)
FROM mapletrade_dev.silver.trade_event_outcomes
GROUP BY outcome
ORDER BY outcome;

SELECT trade_status, count(*)
FROM mapletrade_dev.silver.trade_current
GROUP BY trade_status;

SELECT *
FROM mapletrade_dev.ops.pipeline_reconciliation
ORDER BY checked_at DESC, check_name;
```

A successful Workflow run must have no failed reconciliation checks.

## Tests and current evidence

Verified locally on 2026-08-19:

- 12 unit/integration tests passed;
- one Spark test was skipped because the local machine has no Java/PySpark runtime;
- the Bank of Canada API request and normalization completed successfully; and
- 10,000 base trades produced 11,676 deterministic messages with seed `2026`.

Verified in Databricks Kafka mode on 2026-08-22:

- Workflow run `392859892688787` succeeded in 1 minute 36 seconds;
- restart run `889120302900539` succeeded in 49 seconds without republishing events;
- Bronze and the Silver outcome ledger each contained 11,676 rows;
- Silver Current contained 9,901 authoritative trades;
- Gold contained 100 position rows and 20 currency-notional rows;
- outcomes included applied, duplicate, quarantined, stale-version, and version-conflict cases;
- both runs completed all eight reconciliation checks with zero failures; and
- Bronze, Silver, and Gold counts remained unchanged after restart.

The redacted [execution evidence](docs/evidence/README.md) includes the Confluent topic and Avro
subject, Workflow runs, Kafka metadata, layer and outcome counts, reconciliation results, Gold
samples, and Unity Catalog lineage. The completed acceptance criteria are documented in the
[portfolio evidence checklist](docs/evidence_checklist.md).

## Deliberate MVP boundaries

The MVP excludes Delta Change Data Feed, Terraform, Lakeflow Declarative Pipelines, SCD Type 2,
dashboards, multiple environments, and production alerting. Gold is rebuilt deterministically from
the authoritative Silver current-state table because the demonstrated volume is small and the full
rebuild is easy to verify.

The design remains CDF-ready: Silver Current is the only source of truth, Gold has stable business
keys, and the aggregation functions are reusable. CDF is a future optimization, not a prerequisite
for correctness.

See [roadmap.md](roadmap.md) for extensions and [performance.md](docs/performance.md) for the optional
Spark experiment.
