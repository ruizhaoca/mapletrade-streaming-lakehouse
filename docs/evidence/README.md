# MVP execution evidence

This directory contains redacted evidence from the completed Kafka-mode Databricks MVP. No API
keys, secrets, workspace URLs, or unobscured user email addresses are included.

## Verified executions

| Field | Initial run | Restart validation run |
|---|---:|---:|
| Execution date | 2026-08-22 | 2026-08-22 |
| Source mode | `kafka` | `kafka` |
| Databricks job run ID | `392859892688787` | `889120302900539` |
| Workflow duration | 1 minute 36 seconds | 49 seconds |
| Bronze messages | 11,676 | 11,676 |
| Silver outcome rows | 11,676 | 11,676 |
| Silver current trades | 9,901 | 9,901 |
| Gold position rows | 100 | 100 |
| Gold currency-notional rows | 20 | 20 |
| Reconciliation checks | 8 passed, 0 failed | 8 passed, 0 failed |

The restart run was executed without republishing events. Stable Bronze, Silver, and Gold counts,
together with a second complete reconciliation pass, demonstrate checkpoint recovery and stable
deterministic rebuilds for the tested MVP workload.

## Recorded business results

| Dataset | Result |
|---|---:|
| `silver.trade_event_outcomes` — `APPLIED` | 11,332 |
| `silver.trade_event_outcomes` — `DUPLICATE` | 115 |
| `silver.trade_event_outcomes` — `QUARANTINED` | 127 |
| `silver.trade_event_outcomes` — `STALE_VERSION` | 78 |
| `silver.trade_event_outcomes` — `VERSION_CONFLICT` | 24 |
| `silver.trade_current` — `BOOKED` | 9,389 |
| `silver.trade_current` — `CANCELLED` | 512 |

## Screenshot index

| File | What it verifies |
|---|---|
| [01_workflow_success.png](01_workflow_success.png) | Both Databricks Workflow tasks succeeded in the initial Kafka run. |
| [02_layer_counts.png](02_layer_counts.png) | Bronze, Silver, and Gold row counts after the initial run. |
| [03_event_outcome_counts.png](03_event_outcome_counts.png) | All five event-processing outcome categories and their counts. |
| [04_reconciliation_pass.png](04_reconciliation_pass.png) | Eight initial-run reconciliation checks, all `PASS`. |
| [05_restart_run_success.jpg](05_restart_run_success.jpg) | The second Workflow run succeeded without republishing events. |
| [06_restart_counts_unchanged.png](06_restart_counts_unchanged.png) | Business-table counts remained unchanged after restart. |
| [07_restart_reconciliation_pass.png](07_restart_reconciliation_pass.png) | Both run IDs completed eight checks with zero failures. |
| [08_confluent_topic.png](08_confluent_topic.png) | The three-partition Confluent topic contains trade events. |
| [09_schema_registry_subject.png](09_schema_registry_subject.png) | Avro subject version 1 is associated with the trade-event topic. |
| [10_bronze_kafka_metadata.png](10_bronze_kafka_metadata.png) | Bronze preserves topic, partition, offset, timestamp, and Confluent Avro metadata. |
| [11_trade_status_counts.png](11_trade_status_counts.png) | Current trades resolve to `BOOKED` and `CANCELLED` business states. |
| [12a_gold_net_traded_notional_by_currency_sample.png](12a_gold_net_traded_notional_by_currency_sample.png) | Sample currency-level Gold aggregates and CAD conversion. |
| [12b_gold_position_snapshot_sample.png](12b_gold_position_snapshot_sample.png) | Sample portfolio/instrument position aggregates. |
| [13_unity_catalog_lineage.jpg](13_unity_catalog_lineage.jpg) | Unity Catalog lineage from Silver Current through the booked-trade view to both Gold tables. |
| [14_dashboard_executive_overview_all.png](14_dashboard_executive_overview_all.png) | Published Databricks AI/BI dashboard with both filters set to `All`, including full-workload KPIs, event outcomes, Gold analytics, and eight passing reconciliation checks. |

Together, these artifacts support the repository's Kafka-to-Databricks, lifecycle sequencing,
reconciliation, and tested restart-safety claims.
