"""Cross-layer reconciliation for one month (pure logic; the notebook gathers the numbers)."""

AMOUNT_TOLERANCE = 0.01  # currency/distance sums are compared to within one cent


def evaluate(m: dict) -> list:
    """Return a list of human-readable failures for one month's metrics; empty means everything reconciles.

    Expected keys: bronze_rows, distinct_rows, duplicates_removed, silver_accepted, quarantined, fact_rows,
    fact_total_amount, silver_total_amount, orphans, agg_daily_trips, agg_hourly_trips.
    """
    failures = []
    if m["bronze_rows"] <= 0:
        failures.append("no bronze rows")
    if m["distinct_rows"] > m["bronze_rows"]:
        failures.append(f"distinct rows {m['distinct_rows']} exceed bronze rows {m['bronze_rows']}")
    if m["duplicates_removed"] != m["bronze_rows"] - m["distinct_rows"]:
        failures.append("duplicates_removed does not equal bronze_rows - distinct_rows")
    if m["silver_accepted"] + m["quarantined"] != m["distinct_rows"]:
        failures.append(
            f"silver accepted {m['silver_accepted']} + quarantined {m['quarantined']} != "
            f"distinct bronze rows {m['distinct_rows']}"
        )
    if m["fact_rows"] != m["silver_accepted"]:
        failures.append(f"fact rows {m['fact_rows']} != silver accepted {m['silver_accepted']}")
    if abs(m["fact_total_amount"] - m["silver_total_amount"]) > AMOUNT_TOLERANCE:
        failures.append(f"fact total {m['fact_total_amount']} != silver total {m['silver_total_amount']}")
    if m["orphans"] != 0:
        failures.append(f"{m['orphans']} orphan fact keys")
    for name in ("agg_daily_trips", "agg_hourly_trips"):
        if m[name] != m["fact_rows"]:
            failures.append(f"{name} {m[name]} != fact rows {m['fact_rows']}")
    return failures


def assert_reconciled(m: dict) -> None:
    failures = evaluate(m)
    if failures:
        raise AssertionError("reconciliation failed: " + "; ".join(failures))
