# Design decisions

## One outcome ledger instead of several exception tables

A single physical ledger guarantees one row per source message and makes count/key reconciliation
unambiguous. Views provide convenient quarantine, duplicate, stale, and conflict access without
duplicating schemas and write paths.

## BOOKED and CANCELLED, not ACTIVE and COMPLETED

The project models booking state, not execution and settlement. `NEW` and `AMEND` produce BOOKED;
`CANCEL` produces CANCELLED. Completion would require a separate settlement lifecycle.

## Current state remains physical; booked trades are a view

`silver.trade_current` is authoritative and version-aware. A physical Gold active-trade copy would
repeat the same grain, so `gold.vw_valid_booked_trade` filters it logically.

## Net traded notional, not currency exposure

The project aggregates signed transaction notionals translated into CAD. Without market prices,
sensitivities, or risk factors, calling the result “exposure” would overstate its meaning.

## Full Gold recomputation before CDF

The MVP dataset is small and processed in scheduled `availableNow` runs. Full recomputation is
inexpensive, deterministic, and easy to reconcile. CDF would add checkpoints, pre/post-image
handling, incremental deletion logic, and recovery concerns without solving an MVP bottleneck.

## Unity Catalog has an operational role

The catalog provides three-level names, ownership, comments, managed Delta tables, Volumes, and
lineage. It is used consistently rather than appearing only as an architecture label.

