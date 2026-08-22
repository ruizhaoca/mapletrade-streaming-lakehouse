# MVP execution evidence

This directory contains redacted screenshots from the successful Kafka-mode Databricks run.
Credentials, API keys, secrets, workspace URLs, and user email addresses must never be included.

## Verified run

| Field | Value |
|---|---:|
| Execution date | 2026-08-22 |
| Source mode | `kafka` |
| Databricks job run ID | `392859892688787` |
| Workflow duration | 1 minute 36 seconds |
| Bronze messages | 11,676 |
| Silver outcome rows | 11,676 |
| Silver current trades | 9,901 |
| Gold position rows | 100 |
| Gold currency-notional rows | 20 |
| Reconciliation checks | 8 passed, 0 failed |

## Screenshot files

Save the four existing screenshots with these exact names:

1. `01_workflow_success.png` — both Workflow tasks succeeded.
2. `02_layer_counts.png` — Bronze, Silver, and Gold row counts.
3. `03_event_outcome_counts.png` — processing outcomes by category.
4. `04_reconciliation_pass.png` — latest run's eight reconciliation checks, all `PASS`.

After the restart test, add:

5. `05_restart_run_success.png` — second Workflow run succeeded without republishing events.
6. `06_restart_counts_unchanged.png` — before/after business-table counts are identical and the
   second run has eight passing reconciliation checks.

The first four screenshots prove successful end-to-end processing and reconciliation. The last two
are required before describing the pipeline as restart-safe or replay-safe.

## Optional supporting evidence

For a fuller portfolio walkthrough, also capture the Confluent topic, current trade-status counts,
sample Gold rows, and the Unity Catalog lineage graph. These are useful but are not substitutes for
the successful run and reconciliation evidence above.
