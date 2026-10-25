-- Phase 0: Unity Catalog layout for TaxiLake. Idempotent; safe to re-run.
-- Run in the Databricks SQL editor (Serverless Starter Warehouse) or a notebook %sql cell.
--
-- Fallback in use: Free Edition has no `lakehouse` catalog, so everything lives in the
-- built-in `workspace` catalog. The catalog is not created here.

CREATE SCHEMA IF NOT EXISTS workspace.landing COMMENT 'Raw files as uploaded (volume)';
CREATE SCHEMA IF NOT EXISTS workspace.bronze  COMMENT 'Source data unchanged plus metadata columns';
CREATE SCHEMA IF NOT EXISTS workspace.silver  COMMENT 'Cleaned, typed, validated data and quarantine';
CREATE SCHEMA IF NOT EXISTS workspace.gold    COMMENT 'Star schema and aggregates for Power BI';
CREATE SCHEMA IF NOT EXISTS workspace.ops     COMMENT 'Pipeline run logs and smoke tests';

-- Managed volume for raw TLC Parquet, zone CSV and weather JSON.
-- Files land at /Volumes/workspace/landing/raw/...
CREATE VOLUME IF NOT EXISTS workspace.landing.raw
  COMMENT 'Landing zone for raw source files';

SHOW SCHEMAS IN workspace;
