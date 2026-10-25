-- Phase 4 one-time migration: fact_trips created in Phase 3 has no row_hash column.
-- Databricks does not support ADD COLUMN IF NOT EXISTS, so this file is NOT idempotent: run it once.
-- It fails harmlessly with a 'column already exists' error if the column is already there.
-- New environments get the column from sql/30_gold_tables.sql instead.
-- The first gold run afterwards fills row_hash for existing rows (one-off update), later runs update nothing.

ALTER TABLE workspace.gold.fact_trips ADD COLUMN row_hash BIGINT;
