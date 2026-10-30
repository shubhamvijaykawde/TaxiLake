"""Inspect a saved .pbix for embedded connection values and size, without printing any value.

A .pbix is a zip. Older files keep the Power Query script in a `DataMashup` part (a second zip holding
`Formulas/Section1.m`); newer Power BI Desktop versions store the model in a compressed `DataModel` part instead and
keep the TMDL and DAX scripts, report pages and bindings as separate text parts. This checks:
  - the Power Query script, when a `DataMashup` part exists (parameters must hold the placeholders from
    powerbi/model/taxilake_model.tmdl);
  - every other part except the compressed `DataModel`, as UTF-8 and UTF-16 text, for a Databricks host or path.
The compressed `DataModel` cannot be searched this way, so it is reported as not inspected: a clean result never
proves that the model's own parameter values are placeholders.

Usage: python scripts/check_pbix.py powerbi/TaxiLake.pbix
Exit code 0 = nothing found; 1 = something was found, the file is over 100 MB or it is unreadable.
"""

import io
import re
import struct
import sys
import zipfile
from pathlib import Path

GITHUB_LIMIT_BYTES = 100 * 1000 * 1000
WARN_BYTES = 50 * 1000 * 1000
PARAMETERS = ("ServerHostname", "HttpPath")
PLACEHOLDER_PREFIX = "REPLACE-WITH-"
COMPRESSED_PARTS = ("DataModel", "DataMashup")
_PARAM_RE = re.compile(r'shared\s+(\w+)\s*=\s*"((?:[^"]|"")*)"\s*meta', re.S)
_HOST_RE = re.compile(r"\.cloud\.databricks\.com|\.azuredatabricks\.net|/sql/1\.0/warehouses/", re.I)


def read_section_m(pbix_path):
    """Return the Power Query script (Section1.m) from the DataMashup part, or None if the file has no such part."""
    with zipfile.ZipFile(pbix_path) as outer:
        if "DataMashup" not in outer.namelist():
            return None
        blob = outer.read("DataMashup")
    version, package_len = struct.unpack("<II", blob[:8])
    if version != 0:
        raise ValueError(f"unsupported DataMashup version {version}")
    with zipfile.ZipFile(io.BytesIO(blob[8 : 8 + package_len])) as package:
        return package.read("Formulas/Section1.m").decode("utf-8-sig")


def findings(section_m: str) -> list:
    """Return human-readable findings that never include the offending values."""
    problems = []
    seen = {}
    for name, value in _PARAM_RE.findall(section_m):
        seen[name] = value
    for name in PARAMETERS:
        if name not in seen:
            problems.append(f"parameter {name} not found in the Power Query script")
        elif not seen[name].startswith(PLACEHOLDER_PREFIX):
            problems.append(f"parameter {name} holds a non-placeholder value")
    if _HOST_RE.search(section_m):
        problems.append("the Power Query script contains a Databricks host or warehouse path")
    return problems


def scan_parts(pbix_path) -> list:
    """Return the names of readable parts (not the compressed ones) that contain a Databricks host or path."""
    hits = []
    with zipfile.ZipFile(pbix_path) as outer:
        for name in outer.namelist():
            if name in COMPRESSED_PARTS:
                continue
            data = outer.read(name)
            for text in (data.decode("utf-8", "ignore"), data.decode("utf-16-le", "ignore")):
                if _HOST_RE.search(text):
                    hits.append(name)
                    break
    return hits


def main(argv) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 1
    path = Path(argv[1])
    if not path.exists():
        print(f"FAIL: {path} does not exist")
        return 1
    size = path.stat().st_size
    print(f"size: {size / 1_000_000:.1f} MB")
    if size > GITHUB_LIMIT_BYTES:
        print("FAIL: larger than GitHub's 100 MB per-file limit (use Git LFS or shrink the model)")
    elif size > WARN_BYTES:
        print("WARN: larger than 50 MB; GitHub will warn on push")
    try:
        section_m = read_section_m(path)
        problems = findings(section_m) if section_m is not None else []
        part_hits = scan_parts(path)
    except (KeyError, ValueError, zipfile.BadZipFile) as exc:
        print(f"FAIL: could not read the file ({type(exc).__name__})")
        return 1
    if section_m is None:
        print("note: no DataMashup part (newer file format); the Power Query script cannot be checked separately")
    for problem in problems:
        print("FOUND:", problem)
    for name in part_hits:
        print(f"FOUND: part {name} contains a Databricks host or warehouse path")
    if not problems and not part_hits:
        print("OK: no Databricks host or warehouse path found in the text parts that can be read")
    print("note: the compressed data model part was not inspected, so embedded parameter values there are unknown")
    return 1 if problems or part_hits or size > GITHUB_LIMIT_BYTES else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
