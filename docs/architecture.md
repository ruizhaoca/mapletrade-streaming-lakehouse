# Architecture and correctness model

## Execution sequence

The Workflow has two tasks. Unity Catalog is not a separate processing step; it governs the data
objects read and written inside each task.

### Setup performed once

1. `00_setup_unity_catalog.py` creates the catalog, schemas, tables, views, and Volumes.
2. `01_load_reference_data.py` loads instruments, portfolios, and daily FX rates and records each
   load in `reference.ingestion_manifest`.

### Task 1: `ingest_and_transform`

1. Structured Streaming reads Kafka or the disclosed JSON replay fallback.
2. Bronze stores the untouched payload and source message identity.
3. Avro/JSON is parsed, joined to static reference tables, and validated.
4. `foreachBatch` assigns exactly one outcome to every previously unseen message.
5. Accepted higher versions update `silver.trade_current` through Delta `MERGE`.

### Task 2: `build_gold_and_reconcile`

1. A logical view selects only `BOOKED` current trades.
2. Reusable Spark SQL/DataFrame aggregations fully rebuild the two Gold tables.
3. Independent checks compare Bronze, outcomes, current state, and Gold.
4. Results are appended to `ops.pipeline_reconciliation`; any failed check fails the task.

## Message and business identities

Different keys solve different problems:

| Key | Purpose |
|---|---|
| `topic + partition + offset` | Unique source message and one-outcome guarantee |
| `event_id` | Duplicate business-event delivery detection |
| `trade_id` | Current-state business key and Kafka partitioning key |
| `trade_id + event_version` | Business ordering and same-version conflict detection |
| `payload_hash` | Distinguishes a replay from conflicting business content |

Kafka ordering helps because `trade_id` is the message key, but the target does not trust arrival
order. `event_version` remains authoritative.

## Lifecycle rules

```text
NEW v1  → BOOKED
AMEND   → BOOKED, only at a higher accepted version
CANCEL  → CANCELLED, only at a higher accepted version
```

- A lower version is `STALE_VERSION` and cannot update current state.
- An equivalent same-version replay is `DUPLICATE`.
- Different content at the same trade/version is `VERSION_CONFLICT`; neither conflicting row is
  applied when both appear in the same micro-batch.
- An event after `CANCELLED`, or a second `NEW`, is `QUARANTINED`.
- `AMEND` is a complete replacement snapshot rather than a partial patch.

The failure generator delays only older `AMEND` messages, not the initial `NEW`. Buffering an
orphan `AMEND` that arrives before its `NEW` would require an additional pending-state design and
is deliberately outside the two-day MVP.

## Retry behavior

Structured Streaming checkpoints protect source progress, while Delta `MERGE` protects target
idempotency.

The Silver callback writes the outcome ledger first. It then re-reads all `APPLIED` outcomes for
the same `stream_id` and `batch_id` before merging current state. If the outcome commit succeeds
but the current-state merge or checkpoint commit fails, retrying the batch reuses those durable
outcomes and safely reattempts the version-aware merge.

Delta Lake does not provide one transaction across both tables, so reconciliation is retained as
the final correctness guard.

## Reconciliation meaning

`Spark SQL Reconciliation` performs three independent classes of checks:

1. **Message completeness:** every Bronze source key has exactly one outcome, with no orphan or
   duplicated outcome keys.
2. **Current-state correctness:** the highest `APPLIED` event for every trade matches Current in
   version, status, and payload hash.
3. **Gold correctness:** independently recomputed BOOKED-trade aggregates match every Gold key and
   measure; decimal values allow a CAD 0.01 tolerance.

This is internal pipeline reconciliation, not broker, custodian, settlement, or general-ledger
reconciliation.

## Gold strategy and CDF readiness

The MVP fully rebuilds Gold after each `availableNow` run. At this volume that approach is cheap,
deterministic, easy to recover, and simple to verify.

The implementation preserves a future CDF path without adding CDF complexity now:

- `silver.trade_current` is the only authoritative current-state table;
- Gold transformations are reusable functions;
- both Gold datasets have stable composite keys; and
- full recomputation remains available for bootstrap and recovery.

