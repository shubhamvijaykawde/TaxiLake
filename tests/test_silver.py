from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from taxilake import silver
from taxilake.silver import (
    DROPOFF_BEFORE_PICKUP,
    NEGATIVE_DISTANCE,
    NEGATIVE_FARE,
    NULL_TIMESTAMP,
    PICKUP_OUTSIDE_MONTH,
    ZERO_DISTANCE,
)

FILE = "dbfs:/Volumes/workspace/landing/raw/yellow/2025-01/yellow_tripdata_2025-01.parquet"


def bronze_row(**overrides):
    """One valid January-2025 trip in bronze column names/types; override fields to break a rule."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2025, 1, 15, 8, 0, 0),
        "tpep_dropoff_datetime": datetime(2025, 1, 15, 8, 30, 0),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 132,
        "DOLocationID": 161,
        "payment_type": 1,
        "fare_amount": 12.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 15.5,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.75,
        "_source_file": FILE,
        "_ingested_at": datetime(2026, 10, 25, 16, 0, 0),
        "_batch_id": "t1",
    }
    row.update(overrides)
    return row


BRONZE_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", LongType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_batch_id", StringType()),
    ]
)


def frame(spark, rows):
    """Build a frame; uses the explicit bronze schema when the rows have every bronze column, else infers."""
    if all(set(r) == set(BRONZE_SCHEMA.names) for r in rows):
        return spark.createDataFrame([tuple(r[n] for n in BRONZE_SCHEMA.names) for r in rows], BRONZE_SCHEMA)
    return spark.createDataFrame(rows)


def codes_by_vendor(df):
    return {r["vendor_id"]: sorted(r["reason_codes"]) for r in df.collect()}


def tagged(spark, rows):
    typed = silver.cast_to_silver_schema(silver.standardise_columns(frame(spark, rows)))
    return silver.apply_quality_rules(silver.add_trip_duration(silver.add_source_month(typed)))


# --- standardise / cast / drift -------------------------------------------------------------------------------


def test_standardise_renames_to_snake_case_and_keeps_lineage(spark):
    out = silver.standardise_columns(frame(spark, [bronze_row()]))
    assert out.columns[: len(silver.SOURCE_COLUMNS)] == silver.SOURCE_COLUMNS
    assert "_source_file" in out.columns and "_batch_id" in out.columns
    assert "_ingested_at" not in out.columns
    row = out.collect()[0]
    assert row["pu_location_id"] == 132 and row["airport_fee"] == 0.0


def test_airport_fee_case_variant_is_accepted(spark):
    row = bronze_row()
    row["airport_fee"] = row.pop("Airport_fee")
    assert silver.standardise_columns(frame(spark, [row])).collect()[0]["airport_fee"] == 0.0


def test_missing_optional_columns_become_typed_nulls(spark):
    row = bronze_row()
    del row["cbd_congestion_fee"], row["Airport_fee"], row["congestion_surcharge"]
    out = silver.cast_to_silver_schema(silver.standardise_columns(frame(spark, [row])))
    first = out.collect()[0]
    assert first["cbd_congestion_fee"] is None and first["airport_fee"] is None
    assert dict(out.dtypes)["cbd_congestion_fee"] == "double"


def test_missing_required_column_raises(spark):
    row = bronze_row()
    del row["fare_amount"]
    with pytest.raises(ValueError, match="fare_amount"):
        silver.standardise_columns(frame(spark, [row]))


def test_cast_applies_explicit_types(spark):
    # passenger_count arrives as a double here (type drift between years); inferred schema on purpose.
    row = bronze_row(passenger_count=2.0, VendorID=2)
    out = silver.cast_to_silver_schema(silver.standardise_columns(spark.createDataFrame([row])))
    types = dict(out.dtypes)
    assert types["passenger_count"] == "bigint" and types["vendor_id"] == "int"
    assert types["pickup_ts"] == "timestamp_ntz"


def test_source_month_from_file_name_and_null_when_unrecognised(spark):
    rows = [bronze_row(VendorID=1), bronze_row(VendorID=2, _source_file="somewhere/other.parquet")]
    typed = silver.cast_to_silver_schema(silver.standardise_columns(frame(spark, rows)))
    months = {r["vendor_id"]: r["source_month"] for r in silver.add_source_month(typed).collect()}
    assert months == {1: "2025-01", 2: None}


# --- duration ----------------------------------------------------------------------------------------------


def test_trip_duration_minutes(spark):
    out = tagged(spark, [bronze_row()]).collect()[0]
    assert out["trip_duration_min"] == pytest.approx(30.0)


# --- quality rules -----------------------------------------------------------------------------------------


def test_valid_row_has_no_reason_codes(spark):
    assert tagged(spark, [bronze_row()]).collect()[0]["reason_codes"] == []


def test_each_rule_fires_alone(spark):
    rows = [
        bronze_row(VendorID=1, fare_amount=-5.0),
        bronze_row(VendorID=2, trip_distance=0.0),
        bronze_row(VendorID=3, trip_distance=-1.0),
        bronze_row(
            VendorID=4,
            tpep_pickup_datetime=datetime(2024, 12, 31, 23, 0),
            tpep_dropoff_datetime=datetime(2025, 1, 1, 0, 30),
        ),
        bronze_row(VendorID=5, tpep_dropoff_datetime=datetime(2025, 1, 15, 7, 0)),
    ]
    got = codes_by_vendor(tagged(spark, rows))
    assert got == {
        1: [NEGATIVE_FARE],
        2: [ZERO_DISTANCE],
        3: [NEGATIVE_DISTANCE],
        4: [PICKUP_OUTSIDE_MONTH],
        5: [DROPOFF_BEFORE_PICKUP],
    }


def test_month_boundaries_are_start_inclusive_end_exclusive(spark):
    rows = [
        bronze_row(
            VendorID=1,
            tpep_pickup_datetime=datetime(2025, 1, 1, 0, 0, 0),
            tpep_dropoff_datetime=datetime(2025, 1, 1, 0, 10),
        ),
        bronze_row(
            VendorID=2,
            tpep_pickup_datetime=datetime(2025, 1, 31, 23, 59, 59),
            tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 10),
        ),
        bronze_row(
            VendorID=3,
            tpep_pickup_datetime=datetime(2025, 2, 1, 0, 0, 0),
            tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 10),
        ),
    ]
    got = codes_by_vendor(tagged(spark, rows))
    assert got == {1: [], 2: [], 3: [PICKUP_OUTSIDE_MONTH]}


def test_row_failing_several_rules_keeps_all_codes_once(spark):
    row = bronze_row(fare_amount=-1.0, trip_distance=0.0, tpep_dropoff_datetime=datetime(2025, 1, 15, 7, 0))
    out = tagged(spark, [row])
    assert out.count() == 1
    assert sorted(out.collect()[0]["reason_codes"]) == sorted([NEGATIVE_FARE, ZERO_DISTANCE, DROPOFF_BEFORE_PICKUP])


def test_null_timestamps_are_flagged_not_passed(spark):
    schema_row = bronze_row(VendorID=1, tpep_dropoff_datetime=None)
    pickup_null = bronze_row(VendorID=2, tpep_pickup_datetime=None)
    got = codes_by_vendor(tagged(spark, [schema_row, pickup_null]))
    assert got[1] == [NULL_TIMESTAMP]
    assert got[2] == [NULL_TIMESTAMP]


def test_missing_passenger_count_is_not_a_rule(spark):
    out = tagged(spark, [bronze_row(passenger_count=None)]).collect()[0]
    assert out["passenger_count"] is None and out["reason_codes"] == []


def test_unrecognised_file_name_sends_row_to_quarantine(spark):
    out = tagged(spark, [bronze_row(_source_file="x/unknown.parquet")]).collect()[0]
    assert out["reason_codes"] == [PICKUP_OUTSIDE_MONTH]


# --- duplicates, split and reconciliation -------------------------------------------------------------------


def test_exact_duplicates_are_removed_and_counted(spark):
    base = bronze_row()
    other = bronze_row(VendorID=2)
    typed = silver.cast_to_silver_schema(silver.standardise_columns(frame(spark, [base, base, base, other])))
    deduped, removed = silver.drop_exact_duplicates(silver.add_source_month(typed))
    assert removed == 2 and deduped.count() == 2


def test_rows_differing_in_one_column_are_not_duplicates(spark):
    typed = silver.cast_to_silver_schema(
        silver.standardise_columns(frame(spark, [bronze_row(), bronze_row(tip_amount=3.0)]))
    )
    _, removed = silver.drop_exact_duplicates(silver.add_source_month(typed))
    assert removed == 0


def test_reconciliation_identity_and_nothing_lost(spark):
    good = bronze_row(VendorID=1)
    rows = [
        good,
        good,  # exact duplicate
        bronze_row(VendorID=2, fare_amount=-3.0),
        bronze_row(
            VendorID=3,
            trip_distance=0.0,
            tpep_pickup_datetime=datetime(2025, 3, 1, 0, 0),
            tpep_dropoff_datetime=datetime(2025, 3, 1, 0, 5),
        ),
        bronze_row(VendorID=4),
    ]
    bronze = frame(spark, rows)
    valid, quarantine, removed = silver.transform_bronze(bronze)
    accepted, quarantined = valid.count(), quarantine.count()
    assert removed == 1
    assert accepted + quarantined + removed == bronze.count()
    assert (accepted, quarantined) == (2, 2)
    assert "reason_codes" not in valid.columns
    assert quarantine.filter("size(reason_codes) = 0").count() == 0
    assert {r["vendor_id"] for r in quarantine.collect()} == {2, 3}
    # quarantine keeps the original (bad) values
    bad_fare = quarantine.filter("vendor_id = 2").collect()[0]
    assert bad_fare["fare_amount"] == -3.0
    assert {"source_month", "_source_file", "_batch_id", "_processed_at"} <= set(valid.columns)
