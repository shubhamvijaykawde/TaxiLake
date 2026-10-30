-- Phase 4: operational tables. Idempotent; safe to re-run.
-- month_reconciliation: one row per checks run per month (cross-layer counts and fingerprints). The per-task attempt log
-- added in Phase 7 is ops.pipeline_runs (sql/41_ops_pipeline_runs.sql).

CREATE TABLE IF NOT EXISTS workspace.ops.month_reconciliation (
  source_month          STRING,
  run_batch_id          STRING,
  checked_at            TIMESTAMP,
  bronze_rows           BIGINT,
  distinct_rows         BIGINT,
  duplicates_removed    BIGINT,
  silver_accepted       BIGINT,
  quarantined           BIGINT,
  fact_rows             BIGINT,
  fact_total_amount     DOUBLE,
  silver_total_amount   DOUBLE,
  orphans               BIGINT,
  agg_daily_trips       BIGINT,
  agg_hourly_trips      BIGINT,
  fact_key_hash_sum     DECIMAL(38,0),
  fact_row_hash_sum     DECIMAL(38,0),
  status                STRING,
  failures              STRING
) COMMENT 'Cross-layer reconciliation and content fingerprint per month and checks run';

CREATE TABLE IF NOT EXISTS workspace.ops.failure_injection (
  test_id     STRING,
  created_at  TIMESTAMP
) COMMENT 'Markers for the controlled retry test, a test id with a row here no longer fails';
