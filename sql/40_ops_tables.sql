-- Phase 4: operational tables. Idempotent; safe to re-run.
-- month_reconciliation is a minimal precursor of Phase 7's ops.pipeline_runs: one row per checks run per month.

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
