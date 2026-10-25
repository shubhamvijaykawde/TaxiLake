"""Upload one month of landed files to the Unity Catalog volume with the Databricks CLI.

Authentication comes from the CLI profile (--profile or DATABRICKS_CONFIG_PROFILE); no secrets here.
"""

import argparse
import json
import os
import subprocess
from pathlib import Path

from landing import already_uploaded, volume_path, weather_filenames, yellow_filename

LANDING_DIR = Path(__file__).resolve().parents[1] / "data" / "landing"


def copy(local: Path, remote: str, profile: str | None, force: bool = False) -> None:
    if not local.exists():
        raise FileNotFoundError(f"{local} not found; run download_sources.py / fetch_weather.py first")
    profile_args = ["-p", profile] if profile else []
    parent = remote.rsplit("/", 1)[0]
    if not force:
        listing = subprocess.run(
            ["databricks", "fs", "ls", f"dbfs:{parent}", "-o", "json", *profile_args], capture_output=True, text=True
        )
        if listing.returncode == 0 and already_uploaded(
            json.loads(listing.stdout or "[]"), local.name, local.stat().st_size
        ):
            print("skip (already uploaded, same size):", local.name)
            return
    subprocess.run(["databricks", "fs", "mkdir", f"dbfs:{parent}", *profile_args], check=True)
    print("uploading", local.name, "->", remote)
    subprocess.run(["databricks", "fs", "cp", str(local), f"dbfs:{remote}", "--overwrite", *profile_args], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    parser.add_argument("--force", action="store_true", help="upload even if an identical file is already there")
    args = parser.parse_args()

    yellow = yellow_filename(args.month)
    _, weather = weather_filenames(args.month)
    copy(
        LANDING_DIR / "yellow" / args.month / yellow,
        volume_path("yellow", yellow, args.month),
        args.profile,
        args.force,
    )
    copy(
        LANDING_DIR / "zones" / "taxi_zone_lookup.csv",
        volume_path("zones", "taxi_zone_lookup.csv"),
        args.profile,
        args.force,
    )
    copy(
        LANDING_DIR / "weather" / args.month / weather,
        volume_path("weather", weather, args.month),
        args.profile,
        args.force,
    )


if __name__ == "__main__":
    main()
