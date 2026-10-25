-- Phase 1: bronze tables with explicit schemas. Idempotent; safe to re-run.
-- Source columns and names are kept exactly as published (renaming, casting and drift handling are Phase 2).
-- Column types come from the Parquet footer of yellow_tripdata_2025-01.parquet.
-- Each table adds three metadata columns: _source_file, _ingested_at, _batch_id.

CREATE TABLE IF NOT EXISTS workspace.bronze.yellow_trips (
  VendorID               INT,
  tpep_pickup_datetime   TIMESTAMP_NTZ,
  tpep_dropoff_datetime  TIMESTAMP_NTZ,
  passenger_count        BIGINT,
  trip_distance          DOUBLE,
  RatecodeID             BIGINT,
  store_and_fwd_flag     STRING,
  PULocationID           INT,
  DOLocationID           INT,
  payment_type           BIGINT,
  fare_amount            DOUBLE,
  extra                  DOUBLE,
  mta_tax                DOUBLE,
  tip_amount             DOUBLE,
  tolls_amount           DOUBLE,
  improvement_surcharge  DOUBLE,
  total_amount           DOUBLE,
  congestion_surcharge   DOUBLE,
  Airport_fee            DOUBLE,
  cbd_congestion_fee     DOUBLE,
  _source_file           STRING,
  _ingested_at           TIMESTAMP,
  _batch_id              STRING
) COMMENT 'TLC yellow taxi trips, unchanged from source plus metadata columns';

CREATE TABLE IF NOT EXISTS workspace.bronze.taxi_zones (
  LocationID    INT,
  Borough       STRING,
  Zone          STRING,
  service_zone  STRING,
  _source_file  STRING,
  _ingested_at  TIMESTAMP,
  _batch_id     STRING
) COMMENT 'TLC taxi zone lookup, unchanged from source plus metadata columns';

CREATE TABLE IF NOT EXISTS workspace.bronze.weather_hourly (
  time            STRING,
  temperature_2m  DOUBLE,
  precipitation   DOUBLE,
  wind_speed_10m  DOUBLE,
  weather_code    BIGINT,
  _source_file    STRING,
  _ingested_at    TIMESTAMP,
  _batch_id       STRING
) COMMENT 'Open-Meteo hourly NYC weather (one row per hour), as flattened by scripts/fetch_weather.py';
