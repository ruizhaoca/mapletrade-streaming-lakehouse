# Portfolio evidence checklist

Capture evidence only after the Databricks run succeeds. Redact workspace URLs, user email
addresses, cluster identifiers, and all credentials.

## Required screenshots

1. Confluent topic showing partitions and non-zero message count.
2. Databricks Workflow graph with both tasks succeeded.
3. Bronze sample showing payload format and Kafka metadata.
4. Outcome counts grouped by `outcome`.
5. Current-state counts grouped by `trade_status`.
6. Position and currency-notional Gold samples.
7. Reconciliation rows showing all checks passed.
8. Unity Catalog lineage from Silver Current to the Gold products.

## Required recorded values

- Generated source-message count
- Bronze message count
- Outcome count by category
- Current trade count by status
- Gold group counts
- Workflow run ID and duration
- Restart-test before/after counts
- Executed mode: `kafka` or `replay`

## Claims gate

Do not use “Kafka-to-Databricks,” “restart-safe,” “reconciled,” or measured performance claims on a
résumé until the corresponding evidence above exists.

