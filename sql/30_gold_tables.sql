-- Phase 3: gold star schema with explicit schemas. Idempotent; safe to re-run.
-- Mirrors src/taxilake/gold.py. Dimensions are fully rebuilt by the gold notebook; fact_trips is MERGEd on trip_key;
-- the two aggregate tables are rebuilt per source_month with replaceWhere.

CREATE TABLE IF NOT EXISTS workspace.gold.dim_zone (
  zone_key      INT,
  borough       STRING,
  zone          STRING,
  service_zone  STRING
) COMMENT 'Taxi zones keyed by TLC LocationID (includes Unknown / N/A members)';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_date (
  date_key     INT,
  date         DATE,
  year         INT,
  month        INT,
  month_name   STRING,
  day          INT,
  day_of_week  INT,
  day_name     STRING,
  is_weekend   BOOLEAN
) COMMENT 'Calendar days covering the silver data';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_time_of_day (
  hour_key    INT,
  hour_label  STRING,
  time_band   STRING
) COMMENT 'Hour of day (0-23) with a time band';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_payment_type (
  payment_type_key   INT,
  payment_type_name  STRING
) COMMENT 'TLC payment type codes 0-6';

CREATE TABLE IF NOT EXISTS workspace.gold.fact_trips (
  trip_key               STRING,
  pickup_ts              TIMESTAMP_NTZ,
  dropoff_ts             TIMESTAMP_NTZ,
  pickup_date_key        INT,
  pickup_hour_key        INT,
  pu_zone_key            INT,
  do_zone_key            INT,
  payment_type_key       INT,
  vendor_id              INT,
  ratecode_id            BIGINT,
  passenger_count        BIGINT,
  trip_distance          DOUBLE,
  trip_duration_min      DOUBLE,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  congestion_surcharge   DOUBLE,
  airport_fee            DOUBLE,
  cbd_congestion_fee     DOUBLE,
  total_amount           DOUBLE,
  temperature_c          DOUBLE,
  precipitation_mm       DOUBLE,
  wind_speed_kmh         DOUBLE,
  weather_code           INT,
  source_month           STRING,
  _batch_id              STRING,
  row_hash               BIGINT
) COMMENT 'One row per accepted trip, trip_key = SHA-256 of all source columns plus source file';

CREATE TABLE IF NOT EXISTS workspace.gold.agg_daily_revenue_by_borough (
  source_month     STRING,
  pickup_date_key  INT,
  pickup_borough   STRING,
  trips            BIGINT,
  total_revenue    DOUBLE,
  fare_revenue     DOUBLE,
  tip_revenue      DOUBLE,
  total_distance   DOUBLE
) COMMENT 'Dashboard aggregate: trips and revenue per pickup day and borough';

CREATE TABLE IF NOT EXISTS workspace.gold.agg_hourly_demand (
  source_month       STRING,
  pickup_date_key    INT,
  pickup_hour_key    INT,
  trips              BIGINT,
  total_revenue      DOUBLE,
  avg_trip_distance  DOUBLE,
  temperature_c      DOUBLE,
  precipitation_mm   DOUBLE
) COMMENT 'Dashboard aggregate: trips per pickup day and hour with weather';
