# Portfolio evidence checklist

Capture evidence only after the Databricks run succeeds. Redact workspace URLs, user email
addresses, cluster identifiers, and all credentials.

## Required screenshots — completed 2026-08-22

- [x] Confluent topic showing partitions and a produced trade event.
- [x] Databricks Workflow graph with both tasks succeeded.
- [x] Bronze sample showing payload format and Kafka metadata.
- [x] Outcome counts grouped by `outcome`.
- [x] Current-state counts grouped by `trade_status`.
- [x] Position and currency-notional Gold samples.
- [x] Reconciliation rows showing all checks passed for the initial and restart runs.
- [x] Unity Catalog lineage from Silver Current to both Gold products.

## Required recorded values — completed

- [x] Generated source-message count
- [x] Bronze message count
- [x] Outcome count by category
- [x] Current trade count by status
- [x] Gold group counts
- [x] Workflow run IDs and durations
- [x] Restart-test before/after counts
- [x] Executed mode: `kafka`

## Claims gate

The evidence above supports “Kafka-to-Databricks,” “reconciled,” and tested restart-safety claims
for this MVP workload. It does not support unexecuted scale or performance-improvement claims.
