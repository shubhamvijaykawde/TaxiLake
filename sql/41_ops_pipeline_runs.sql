-- Phase 7: one row per task attempt. Append-only; idempotent to re-run (the table is only created if it is missing).
-- Apply before deploying notebooks that log to it: a task whose log write fails is failed deliberately (see src/taxilake/runlog.py).
-- NULL in a count column means "not applicable for this task", never zero.

CREATE TABLE IF NOT EXISTS workspace.ops.pipeline_runs (
  source_month      STRING,
  batch_id          STRING,
  task_key          STRING,
  attempt           INT,
  job_run_id        STRING,
  task_run_id       STRING,
  started_at        TIMESTAMP,
  finished_at       TIMESTAMP,
  duration_s        DOUBLE,
  status            STRING,
  rows_read         BIGINT,
  rows_written      BIGINT,
  rows_quarantined  BIGINT,
  details           STRING,
  error_message     STRING
) COMMENT 'One row per pipeline task attempt: month, batch, attempt, timings, status and row counts (read, written, quarantined)';
