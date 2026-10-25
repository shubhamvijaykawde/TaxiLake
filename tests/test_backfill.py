import pytest

from taxilake.backfill import check_schema, month_range, pending_months

GOOD = {
    "VendorID": "int32",
    "tpep_pickup_datetime": "timestamp[us]",
    "tpep_dropoff_datetime": "timestamp[us]",
    "passenger_count": "int64",
    "trip_distance": "double",
    "RatecodeID": "int64",
    "store_and_fwd_flag": "large_string",
    "PULocationID": "int32",
    "DOLocationID": "int32",
    "payment_type": "int64",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
    "cbd_congestion_fee": "double",
}


def test_month_range_inclusive_and_crosses_year_end():
    assert month_range("2025-01", "2025-03") == ["2025-01", "2025-02", "2025-03"]
    assert month_range("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert month_range("2025-05", "2025-05") == ["2025-05"]


@pytest.mark.parametrize(
    "start, end", [("2025-06", "2025-01"), ("2025-1", "2025-02"), ("2025-01", "2025-13"), ("", "2025-01")]
)
def test_month_range_rejects_bad_input(start, end):
    with pytest.raises(ValueError):
        month_range(start, end)


def test_pending_months_skips_reconciled_and_keeps_order():
    months = ["2025-01", "2025-02", "2025-03", "2025-04"]
    assert pending_months(months, {"2025-01", "2025-03"}) == ["2025-02", "2025-04"]
    assert pending_months(months, set()) == months
    assert pending_months(months, set(months)) == []


def test_good_schema_has_no_problems():
    assert check_schema(GOOD) == []


def test_optional_columns_may_be_absent_and_case_is_ignored():
    older = {k: v for k, v in GOOD.items() if k not in ("cbd_congestion_fee", "Airport_fee", "congestion_surcharge")}
    assert check_schema(older) == []
    renamed = {("airport_fee" if k == "Airport_fee" else k): v for k, v in GOOD.items()}
    assert check_schema(renamed) == []


def test_type_drift_is_rejected():
    drifted = dict(GOOD, passenger_count="double")
    problems = check_schema(drifted)
    assert len(problems) == 1 and "passenger_count" in problems[0]


def test_missing_required_and_unexpected_columns_are_rejected():
    broken = {k: v for k, v in GOOD.items() if k != "fare_amount"}
    broken["surprise_fee"] = "double"
    problems = check_schema(broken)
    assert any("fare_amount" in p for p in problems)
    assert any("surprise_fee" in p for p in problems)
