-- Phase 2: silver tables with explicit schemas. Idempotent; safe to re-run.
-- Column names/types mirror src/taxilake/silver.py (SILVER_SOURCE_SCHEMA plus derived and lineage columns).
-- silver.trips carries CHECK constraints as a backstop: anything that breaks them must have been quarantined.
-- The silver notebook only writes to these tables; it must not be the first thing to create them (no constraints then).

CREATE TABLE IF NOT EXISTS workspace.silver.trips (
  vendor_id              INT,
  pickup_ts              TIMESTAMP_NTZ,
  dropoff_ts             TIMESTAMP_NTZ,
  passenger_count        BIGINT,
  trip_distance          DOUBLE,
  ratecode_id            BIGINT,
  store_and_fwd_flag     STRING,
  pu_location_id         INT,
  do_location_id         INT,
  payment_type           BIGINT,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  total_amount           DOUBLE,
  congestion_surcharge   DOUBLE,
  airport_fee            DOUBLE,
  cbd_congestion_fee     DOUBLE,
  trip_duration_min      DOUBLE,
  source_month           STRING,
  _source_file           STRING,
  _batch_id              STRING,
  _processed_at          TIMESTAMP
) COMMENT 'Validated, typed yellow taxi trips';

-- Databricks does not accept CHECK constraints inside CREATE TABLE, so they are added afterwards.
-- DROP ... IF EXISTS then ADD keeps the script idempotent.
ALTER TABLE workspace.silver.trips DROP CONSTRAINT IF EXISTS trips_fare_non_negative;
ALTER TABLE workspace.silver.trips ADD CONSTRAINT trips_fare_non_negative CHECK (fare_amount >= 0);
ALTER TABLE workspace.silver.trips DROP CONSTRAINT IF EXISTS trips_distance_positive;
ALTER TABLE workspace.silver.trips ADD CONSTRAINT trips_distance_positive CHECK (trip_distance > 0);
ALTER TABLE workspace.silver.trips DROP CONSTRAINT IF EXISTS trips_dropoff_after_pickup;
ALTER TABLE workspace.silver.trips ADD CONSTRAINT trips_dropoff_after_pickup CHECK (dropoff_ts >= pickup_ts);

CREATE TABLE IF NOT EXISTS workspace.silver.trips_quarantine (
  vendor_id              INT,
  pickup_ts              TIMESTAMP_NTZ,
  dropoff_ts             TIMESTAMP_NTZ,
  passenger_count        BIGINT,
  trip_distance          DOUBLE,
  ratecode_id            BIGINT,
  store_and_fwd_flag     STRING,
  pu_location_id         INT,
  do_location_id         INT,
  payment_type           BIGINT,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  total_amount           DOUBLE,
  congestion_surcharge   DOUBLE,
  airport_fee            DOUBLE,
  cbd_congestion_fee     DOUBLE,
  trip_duration_min      DOUBLE,
  source_month           STRING,
  _source_file           STRING,
  _batch_id              STRING,
  _processed_at          TIMESTAMP,
  reason_codes           ARRAY<STRING>
) COMMENT 'Trips that failed a quality rule, with original values and stable reason codes';
