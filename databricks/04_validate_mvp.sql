-- Databricks notebook source
-- Run after each Workflow execution to collect portfolio and restart-test evidence.

-- COMMAND ----------

SELECT 'bronze.trade_events' AS object_name, COUNT(*) AS row_count
FROM mapletrade_dev.bronze.trade_events
UNION ALL
SELECT 'silver.trade_event_outcomes', COUNT(*)
FROM mapletrade_dev.silver.trade_event_outcomes
UNION ALL
SELECT 'silver.trade_current', COUNT(*)
FROM mapletrade_dev.silver.trade_current
UNION ALL
SELECT 'gold.position_snapshot', COUNT(*)
FROM mapletrade_dev.gold.position_snapshot
UNION ALL
SELECT 'gold.net_traded_notional_by_currency', COUNT(*)
FROM mapletrade_dev.gold.net_traded_notional_by_currency;

-- COMMAND ----------

SELECT outcome, COUNT(*) AS event_count
FROM mapletrade_dev.silver.trade_event_outcomes
GROUP BY outcome
ORDER BY outcome;

-- COMMAND ----------

SELECT trade_status, COUNT(*) AS trade_count
FROM mapletrade_dev.silver.trade_current
GROUP BY trade_status
ORDER BY trade_status;

-- COMMAND ----------

WITH latest_run AS (
  SELECT pipeline_run_id
  FROM mapletrade_dev.ops.pipeline_reconciliation
  ORDER BY checked_at DESC
  LIMIT 1
)
SELECT
  r.pipeline_run_id,
  r.check_name,
  r.source_value,
  r.target_value,
  r.difference,
  r.status,
  r.details
FROM mapletrade_dev.ops.pipeline_reconciliation AS r
INNER JOIN latest_run AS l
  ON r.pipeline_run_id = l.pipeline_run_id
ORDER BY r.check_name;

-- COMMAND ----------

SELECT
  pipeline_run_id,
  COUNT(*) AS check_count,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed_check_count,
  SUM(CASE WHEN status <> 'PASS' THEN 1 ELSE 0 END) AS failed_check_count,
  MAX(checked_at) AS checked_at
FROM mapletrade_dev.ops.pipeline_reconciliation
GROUP BY pipeline_run_id
ORDER BY checked_at DESC;
