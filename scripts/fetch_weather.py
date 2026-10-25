"""Fetch hourly NYC weather for one month from Open-Meteo (no API key) into data/landing/weather/."""

import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

from landing import NYC_LAT, NYC_LON, WEATHER_URL, WEATHER_VARS, flatten_hourly, month_bounds, weather_filenames

LANDING_DIR = Path(__file__).resolve().parents[1] / "data" / "landing"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    start, end = month_bounds(args.month)
    query = urllib.parse.urlencode(
        {
            "latitude": NYC_LAT,
            "longitude": NYC_LON,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": ",".join(WEATHER_VARS),
            "timezone": "America/New_York",
        }
    )
    with urllib.request.urlopen(f"{WEATHER_URL}?{query}", timeout=60) as resp:
        payload = json.load(resp)

    rows = flatten_hourly(payload)
    raw_name, jsonl_name = weather_filenames(args.month)
    out_dir = LANDING_DIR / "weather" / args.month
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / raw_name).write_text(json.dumps(payload), encoding="utf-8")
    with open(out_dir / jsonl_name, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    print(f"wrote {len(rows)} hourly rows to {out_dir / jsonl_name}")


if __name__ == "__main__":
    main()
