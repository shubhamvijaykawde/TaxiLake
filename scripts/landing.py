"""Pure helpers shared by the landing scripts (no network or CLI calls, so they are unit-testable)."""

import calendar
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxilake.namespace import resolve  # noqa: E402

YELLOW_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060
WEATHER_VARS = ("temperature_2m", "precipitation", "wind_speed_10m", "weather_code")
CATALOG = "workspace"

_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_month(value: str) -> tuple[int, int]:
    """Return (year, month) for 'YYYY-MM'; raise ValueError for anything else."""
    match = _MONTH_RE.match(value or "")
    if not match:
        raise ValueError(f"month must look like YYYY-MM, got {value!r}")
    year, month = int(match.group(1)), int(match.group(2))
    if year < 2009:
        raise ValueError(f"TLC trip records start in 2009, got {value!r}")
    return year, month


def build_yellow_url(month: str) -> str:
    parse_month(month)
    return YELLOW_URL.format(month=month)


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = parse_month(month)
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


def yellow_filename(month: str) -> str:
    parse_month(month)
    return f"yellow_tripdata_{month}.parquet"


def weather_filenames(month: str) -> tuple[str, str]:
    """(raw API json, flattened jsonl) file names for a month."""
    parse_month(month)
    return f"weather_{month}_raw.json", f"weather_{month}.jsonl"


def volume_path(kind: str, filename: str, month: str | None = None, *, environment: str) -> str:
    """Target path in the environment's landing volume for one file (there is deliberately no default environment)."""
    if kind not in {"yellow", "zones", "weather"}:
        raise ValueError(f"unknown kind {kind!r}")
    parts = [resolve(CATALOG, environment).volume_root, kind]
    if month is not None:
        parse_month(month)
        parts.append(month)
    parts.append(filename)
    return "/".join(parts)


def already_uploaded(listing: list, name: str, local_size: int) -> bool:
    """True if `databricks fs ls -o json` output already holds a file with this name and size.

    COPY INTO tracks files it has loaded, and re-uploading an identical file would only change its timestamp,
    so identical files are skipped (use --force to upload anyway).
    """
    return any(e.get("name") == name and not e.get("is_directory") and e.get("size") == local_size for e in listing)


def flatten_hourly(response: dict) -> list[dict]:
    """Turn Open-Meteo's column-oriented 'hourly' block into one dict per hour.

    Missing values stay None. Raises ValueError if the arrays have different lengths.
    """
    hourly = response.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError("response has no hourly.time block")
    times = hourly["time"]
    for var in WEATHER_VARS:
        values = hourly.get(var)
        if values is None or len(values) != len(times):
            raise ValueError(f"hourly.{var} missing or length differs from time ({len(times)})")
    return [{"time": t, **{var: hourly[var][i] for var in WEATHER_VARS}} for i, t in enumerate(times)]
