"""Pure helpers for the backfill driver (no network, CLI or Spark)."""

import re

_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")

# Acceptable pyarrow type strings for each bronze source column (matched case-insensitively by name).
EXPECTED_TYPES = {
    "vendorid": {"int32"},
    "tpep_pickup_datetime": {"timestamp[us]"},
    "tpep_dropoff_datetime": {"timestamp[us]"},
    "passenger_count": {"int64"},
    "trip_distance": {"double"},
    "ratecodeid": {"int64"},
    "store_and_fwd_flag": {"string", "large_string"},
    "pulocationid": {"int32"},
    "dolocationid": {"int32"},
    "payment_type": {"int64"},
    "fare_amount": {"double"},
    "extra": {"double"},
    "mta_tax": {"double"},
    "tip_amount": {"double"},
    "tolls_amount": {"double"},
    "improvement_surcharge": {"double"},
    "total_amount": {"double"},
    "congestion_surcharge": {"double"},
    "airport_fee": {"double"},
    "cbd_congestion_fee": {"double"},
}
OPTIONAL = {"congestion_surcharge", "airport_fee", "cbd_congestion_fee"}


def _parse(month: str) -> tuple:
    match = _MONTH_RE.match(month or "")
    if not match:
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return int(match.group(1)), int(match.group(2))


def month_range(start: str, end: str) -> list:
    """All months from start to end inclusive; raises if the range is reversed or malformed."""
    (y, m), (ey, em) = _parse(start), _parse(end)
    if (y, m) > (ey, em):
        raise ValueError(f"start {start} is after end {end}")
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def pending_months(months: list, reconciled: set) -> list:
    """Months still to run, in order, skipping those already reconciled (used by --resume)."""
    return [m for m in months if m not in reconciled]


def check_schema(columns: dict) -> list:
    """Compare {column name: arrow type string} with the bronze expectations; return a list of problems."""
    problems = []
    seen = set()
    for name, dtype in columns.items():
        key = name.lower()
        seen.add(key)
        if key not in EXPECTED_TYPES:
            problems.append(f"unexpected column {name} ({dtype})")
        elif dtype not in EXPECTED_TYPES[key]:
            problems.append(f"column {name} has type {dtype}, expected one of {sorted(EXPECTED_TYPES[key])}")
    for key in EXPECTED_TYPES:
        if key not in seen and key not in OPTIONAL:
            problems.append(f"required column {key} is missing")
    return problems
