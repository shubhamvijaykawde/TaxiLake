from datetime import date, datetime

import pytest
from pyspark.sql import functions as F
from test_silver import bronze_row, frame

from taxilake import gold, silver


def silver_trips(spark, rows):
    valid, _, _ = silver.transform_bronze(frame(spark, rows))
    return valid


def zones(spark, ids=(132, 161, 264, 265)):
    data = [
        (i, "Queens" if i == 132 else "Manhattan" if i == 161 else "Unknown", f"Zone {i}", "Boro Zone") for i in ids
    ]
    return spark.createDataFrame(data, "LocationID int, Borough string, Zone string, service_zone string")


def weather(spark, hours=(8,)):
    data = [(f"2025-01-15T{h:02d}:00", 1.5, 0.2, 10.0, 3) for h in hours]
    return spark.createDataFrame(
        data, "time string, temperature_2m double, precipitation double, wind_speed_10m double, weather_code long"
    )


# --- trip key ----------------------------------------------------------------------------------------------


def test_trip_key_is_stable_and_unique_for_distinct_rows(spark):
    rows = [bronze_row(VendorID=1), bronze_row(VendorID=2), bronze_row(VendorID=3, tip_amount=9.0)]
    keyed = gold.add_trip_key(silver_trips(spark, rows))
    assert keyed.select("trip_key").distinct().count() == 3
    again = gold.add_trip_key(silver_trips(spark, rows))
    assert {r["trip_key"] for r in keyed.collect()} == {r["trip_key"] for r in again.collect()}
    assert all(len(r["trip_key"]) == 64 for r in keyed.collect())


def test_trip_key_is_null_safe(spark):
    # A null in one column and a null in another must not hash alike (concat_ws would skip both nulls).
    rows = [bronze_row(VendorID=1, extra=None, mta_tax=0.5), bronze_row(VendorID=1, extra=0.5, mta_tax=None)]
    keyed = gold.add_trip_key(silver_trips(spark, rows))
    assert keyed.select("trip_key").distinct().count() == 2


# --- dimensions --------------------------------------------------------------------------------------------


def test_dim_zone_keeps_unknown_members_and_unique_keys(spark):
    dim = gold.build_dim_zone(zones(spark))
    assert dim.count() == 4
    assert dim.select("zone_key").distinct().count() == 4
    assert {r["zone_key"] for r in dim.collect()} >= {264, 265}


def test_dim_date_covers_range_and_flags_weekends(spark):
    dim = gold.build_dim_date(spark, date(2025, 1, 1), date(2025, 1, 31))
    rows = {r["date"]: r for r in dim.collect()}
    assert len(rows) == 31
    assert rows[date(2025, 1, 1)]["date_key"] == 20250101
    assert rows[date(2025, 1, 4)]["is_weekend"] is True  # Saturday
    assert rows[date(2025, 1, 5)]["day_of_week"] == 7  # Sunday
    assert rows[date(2025, 1, 6)]["is_weekend"] is False  # Monday
    assert rows[date(2025, 1, 6)]["day_of_week"] == 1


def test_dim_time_of_day_bands_match_reference(spark):
    dim = {r["hour_key"]: r for r in gold.build_dim_time_of_day(spark).collect()}
    assert sorted(dim) == list(range(24))
    for hour, row in dim.items():
        assert row["time_band"] == gold.time_band(hour)
    assert (dim[5]["time_band"], dim[6]["time_band"]) == ("night", "morning")
    assert (dim[11]["time_band"], dim[12]["time_band"]) == ("morning", "afternoon")
    assert (dim[17]["time_band"], dim[18]["time_band"]) == ("afternoon", "evening")
    assert dim[8]["hour_label"] == "08:00"
    with pytest.raises(ValueError):
        gold.time_band(24)


def test_dim_payment_type_covers_documented_codes(spark):
    keys = sorted(r["payment_type_key"] for r in gold.build_dim_payment_type(spark).collect())
    assert keys == [0, 1, 2, 3, 4, 5, 6]


# --- fact ----------------------------------------------------------------------------------------------------


def test_fact_row_shape_and_keys(spark):
    fact = gold.build_fact_trips(silver_trips(spark, [bronze_row()]), weather(spark))
    assert fact.columns == gold.FACT_COLUMNS
    row = fact.collect()[0]
    assert row["pickup_date_key"] == 20250115 and row["pickup_hour_key"] == 8
    assert (row["pu_zone_key"], row["do_zone_key"], row["payment_type_key"]) == (132, 161, 1)
    assert row["temperature_c"] == 1.5 and row["weather_code"] == 3


def test_hour_truncation_boundaries(spark):
    rows = [
        bronze_row(
            VendorID=1,
            tpep_pickup_datetime=datetime(2025, 1, 15, 8, 59, 59),
            tpep_dropoff_datetime=datetime(2025, 1, 15, 9, 20),
        ),
        bronze_row(
            VendorID=2,
            tpep_pickup_datetime=datetime(2025, 1, 15, 9, 0, 0),
            tpep_dropoff_datetime=datetime(2025, 1, 15, 9, 20),
        ),
    ]
    fact = gold.build_fact_trips(silver_trips(spark, rows), weather(spark, hours=(8, 9)))
    assert sorted(r["pickup_hour_key"] for r in fact.collect()) == [8, 9]
    assert fact.where(F.col("temperature_c").isNull()).count() == 0


def test_missing_weather_keeps_trip_with_null_weather(spark):
    fact = gold.build_fact_trips(silver_trips(spark, [bronze_row()]), weather(spark, hours=(3,)))
    assert fact.count() == 1
    row = fact.collect()[0]
    assert row["temperature_c"] is None and row["weather_code"] is None


def test_unknown_zone_ids_map_to_unknown_dimension_rows(spark):
    fact = gold.build_fact_trips(silver_trips(spark, [bronze_row(PULocationID=264, DOLocationID=265)]), weather(spark))
    dim = gold.build_dim_zone(zones(spark))
    gold.assert_no_orphans(fact, dim, "pu_zone_key", "zone_key", "dim_zone")
    gold.assert_no_orphans(fact, dim, "do_zone_key", "zone_key", "dim_zone")


def test_orphan_check_raises_when_a_key_has_no_dimension_row(spark):
    fact = gold.build_fact_trips(silver_trips(spark, [bronze_row(PULocationID=999)]), weather(spark))
    with pytest.raises(ValueError, match="dim_zone"):
        gold.assert_no_orphans(fact, gold.build_dim_zone(zones(spark)), "pu_zone_key", "zone_key", "dim_zone")


def test_orphan_check_flags_unknown_payment_type(spark):
    fact = gold.build_fact_trips(silver_trips(spark, [bronze_row(payment_type=9)]), weather(spark))
    with pytest.raises(ValueError, match="dim_payment_type"):
        gold.assert_no_orphans(
            fact, gold.build_dim_payment_type(spark), "payment_type_key", "payment_type_key", "dim_payment_type"
        )


def test_duplicate_weather_rows_would_fan_out_and_are_detectable(spark):
    # Documents why callers reconcile fact rows to silver rows: two weather rows for one hour duplicate the trip.
    wx = weather(spark).union(weather(spark))
    trips = silver_trips(spark, [bronze_row()])
    assert gold.build_fact_trips(trips, wx).count() != trips.count()


# --- aggregates ------------------------------------------------------------------------------------------------


def test_aggregates_sum_back_to_fact(spark):
    rows = [
        bronze_row(VendorID=1, total_amount=10.0, fare_amount=8.0),
        bronze_row(VendorID=2, total_amount=20.0, fare_amount=15.0),
        bronze_row(VendorID=3, PULocationID=161, total_amount=5.0, fare_amount=4.0),
    ]
    fact = gold.build_fact_trips(silver_trips(spark, rows), weather(spark))
    daily = gold.daily_revenue_by_borough(fact, gold.build_dim_zone(zones(spark)))
    hourly = gold.hourly_demand(fact)
    total = fact.agg(F.sum("total_amount").alias("rev"), F.count("*").alias("n")).collect()[0]
    for agg in (daily, hourly):
        got = agg.agg(F.sum("total_revenue").alias("rev"), F.sum("trips").alias("n")).collect()[0]
        assert got["n"] == total["n"] == 3
        assert got["rev"] == pytest.approx(total["rev"])
    boroughs = {r["pickup_borough"]: r["trips"] for r in daily.collect()}
    assert boroughs == {"Queens": 2, "Manhattan": 1}


# --- row hash (Phase 4: skip unchanged rows in MERGE) -----------------------------------------------------------


def test_row_hash_is_stable_and_ignores_batch_id(spark):
    trips = silver_trips(spark, [bronze_row()])
    first = gold.build_fact_trips(trips, weather(spark)).collect()[0]
    second = gold.build_fact_trips(trips.withColumn("_batch_id", F.lit("another-batch")), weather(spark)).collect()[0]
    assert first["row_hash"] == second["row_hash"]
    assert first["_batch_id"] != second["_batch_id"]


def test_row_hash_changes_when_a_measure_or_weather_changes(spark):
    base = gold.build_fact_trips(silver_trips(spark, [bronze_row()]), weather(spark)).collect()[0]["row_hash"]
    tipped = gold.build_fact_trips(silver_trips(spark, [bronze_row(tip_amount=9.0)]), weather(spark)).collect()[0][
        "row_hash"
    ]
    other_wx = weather(spark).withColumn("temperature_2m", F.lit(-3.0))
    reweathered = gold.build_fact_trips(silver_trips(spark, [bronze_row()]), other_wx).collect()[0]["row_hash"]
    assert len({base, tipped, reweathered}) == 3


def test_count_orphans_counts_missing_dimension_rows(spark):
    fact = gold.build_fact_trips(
        silver_trips(spark, [bronze_row(PULocationID=999), bronze_row(VendorID=2)]), weather(spark)
    )
    assert gold.count_orphans(fact, gold.build_dim_zone(zones(spark)), "pu_zone_key", "zone_key") == 1
