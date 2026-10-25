"""Download one month of TLC yellow-taxi Parquet plus the zone lookup CSV into data/landing/ (gitignored)."""

import argparse
import urllib.request
from pathlib import Path

from landing import ZONES_URL, build_yellow_url, yellow_filename

LANDING_DIR = Path(__file__).resolve().parents[1] / "data" / "landing"


def download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(url, timeout=120) as resp:
        expected = int(resp.headers.get("Content-Length", -1))
        with open(tmp, "wb") as out:
            while chunk := resp.read(1 << 20):
                out.write(chunk)
    if expected >= 0 and tmp.stat().st_size != expected:
        tmp.unlink()
        raise IOError(f"size mismatch for {url}: expected {expected} bytes")
    tmp.replace(target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    yellow = LANDING_DIR / "yellow" / args.month / yellow_filename(args.month)
    zones = LANDING_DIR / "zones" / "taxi_zone_lookup.csv"
    for url, target in ((build_yellow_url(args.month), yellow), (ZONES_URL, zones)):
        if target.exists():
            print(f"skip (already present): {target}")
            continue
        print(f"downloading {url}")
        download(url, target)
        print(f"saved {target} ({target.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
