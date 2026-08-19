# Optional Spark performance experiment

This experiment is intentionally outside the critical two-day path. Run it only after the complete
Workflow and reconciliation pass.

## Question

Does broadcasting the small instrument reference reduce shuffle and runtime compared with a
standard join on a one-million-event replay dataset?

## Procedure

1. Generate or replicate approximately one million valid events.
2. Select only the required trade and instrument columns.
3. Run a baseline join without an explicit hint.
4. Run the optimized join with `broadcast(reference.instruments)`.
5. Capture `EXPLAIN FORMATTED`, Databricks Query Profile, runtime, task count, and shuffle bytes.
6. Repeat both runs after cache clearing or use multiple alternating runs to reduce warm-cache bias.

## Reporting template

| Measurement | Baseline | Optimized |
|---|---:|---:|
| Runtime | TBD | TBD |
| Shuffle read | TBD | TBD |
| Shuffle write | TBD | TBD |
| Tasks | TBD | TBD |
| Physical join | TBD | TBD |

Do not publish a percentage improvement until the experiment is executed and reproducible.

