from datetime import date

import pytest
from landing import (
    build_yellow_url,
    flatten_hourly,
    month_bounds,
    parse_month,
    volume_path,
    weather_filenames,
    yellow_filename,
)


@pytest.mark.parametrize("bad", ["2025-13", "2025-00", "25-01", "2025/01", "", None, "2008-12", "2025-1"])
def test_parse_month_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        parse_month(bad)


def test_parse_month_ok():
    assert parse_month("2025-01") == (2025, 1)


def test_build_yellow_url():
    assert build_yellow_url("2025-01").endswith("/trip-data/yellow_tripdata_2025-01.parquet")
    with pytest.raises(ValueError):
        build_yellow_url("2025-1")


def test_month_bounds_handles_leap_year():
    assert month_bounds("2024-02") == (date(2024, 2, 1), date(2024, 2, 29))
    assert month_bounds("2025-02")[1] == date(2025, 2, 28)
    assert month_bounds("2025-01") == (date(2025, 1, 1), date(2025, 1, 31))


def test_filenames():
    assert yellow_filename("2025-01") == "yellow_tripdata_2025-01.parquet"
    assert weather_filenames("2025-01") == ("weather_2025-01_raw.json", "weather_2025-01.jsonl")


def test_volume_path():
    assert volume_path("yellow", "f.parquet", "2025-01") == "/Volumes/workspace/landing/raw/yellow/2025-01/f.parquet"
    assert volume_path("zones", "z.csv") == "/Volumes/workspace/landing/raw/zones/z.csv"
    with pytest.raises(ValueError):
        volume_path("green", "f.parquet")


def _response(n, **overrides):
    hourly = {
        "time": [f"2025-01-01T{h:02d}:00" for h in range(n)],
        "temperature_2m": [1.0] * n,
        "precipitation": [0.0] * n,
        "wind_speed_10m": [5.0] * n,
        "weather_code": [3] * n,
    }
    hourly.update(overrides)
    return {"hourly": hourly}


def test_flatten_hourly_one_row_per_hour_and_keeps_nulls():
    rows = flatten_hourly(_response(3, precipitation=[0.0, None, 0.2]))
    assert len(rows) == 3
    assert rows[1]["precipitation"] is None
    assert rows[0] == {
        "time": "2025-01-01T00:00",
        "temperature_2m": 1.0,
        "precipitation": 0.0,
        "wind_speed_10m": 5.0,
        "weather_code": 3,
    }


def test_flatten_hourly_rejects_missing_or_misaligned_arrays():
    with pytest.raises(ValueError):
        flatten_hourly({})
    with pytest.raises(ValueError):
        flatten_hourly(_response(3, temperature_2m=[1.0]))
    bad = _response(3)
    del bad["hourly"]["weather_code"]
    with pytest.raises(ValueError):
        flatten_hourly(bad)


def test_already_uploaded_matches_name_and_size_only():
    from landing import already_uploaded

    listing = [
        {"name": "a.parquet", "is_directory": False, "size": 10},
        {"name": "dir", "is_directory": True, "size": 0},
    ]
    assert already_uploaded(listing, "a.parquet", 10) is True
    assert already_uploaded(listing, "a.parquet", 11) is False  # changed file is re-uploaded
    assert already_uploaded(listing, "b.parquet", 10) is False
    assert already_uploaded(listing, "dir", 0) is False
    assert already_uploaded([], "a.parquet", 10) is False
