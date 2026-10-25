import pytest

from taxilake import reconcile
from taxilake.injection import should_fail


def balanced(**overrides):
    m = {
        "bronze_rows": 100,
        "distinct_rows": 98,
        "duplicates_removed": 2,
        "silver_accepted": 90,
        "quarantined": 8,
        "fact_rows": 90,
        "fact_total_amount": 1000.0,
        "silver_total_amount": 1000.0,
        "orphans": 0,
        "agg_daily_trips": 90,
        "agg_hourly_trips": 90,
    }
    m.update(overrides)
    return m


def test_balanced_month_has_no_failures():
    assert reconcile.evaluate(balanced()) == []
    reconcile.assert_reconciled(balanced())


@pytest.mark.parametrize(
    "overrides, fragment",
    [
        ({"quarantined": 7}, "quarantined"),
        ({"silver_accepted": 91}, "silver accepted"),
        ({"fact_rows": 89}, "fact rows"),
        ({"fact_total_amount": 1000.5}, "fact total"),
        ({"orphans": 3}, "orphan"),
        ({"agg_daily_trips": 89}, "agg_daily_trips"),
        ({"agg_hourly_trips": 91}, "agg_hourly_trips"),
        ({"duplicates_removed": 1}, "duplicates_removed"),
        ({"distinct_rows": 101, "duplicates_removed": -1}, "exceed"),
        (
            {
                "bronze_rows": 0,
                "distinct_rows": 0,
                "duplicates_removed": 0,
                "silver_accepted": 0,
                "quarantined": 0,
                "fact_rows": 0,
                "agg_daily_trips": 0,
                "agg_hourly_trips": 0,
            },
            "no bronze rows",
        ),
    ],
)
def test_each_mismatch_is_reported(overrides, fragment):
    failures = reconcile.evaluate(balanced(**overrides))
    assert any(fragment in f for f in failures), failures
    with pytest.raises(AssertionError):
        reconcile.assert_reconciled(balanced(**overrides))


def test_amount_difference_within_tolerance_passes():
    assert reconcile.evaluate(balanced(fact_total_amount=1000.005)) == []


@pytest.mark.parametrize(
    "test_id, marker_exists, expected",
    [("", False, False), ("", True, False), ("t1", False, True), ("t1", True, False)],
)
def test_should_fail_truth_table(test_id, marker_exists, expected):
    assert should_fail(test_id, marker_exists) is expected
