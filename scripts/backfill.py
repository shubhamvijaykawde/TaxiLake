"""Backfill driver: land one month at a time, run the cloud pipeline, and verify reconciliation.

Runs sequentially on purpose (free-tier quota, easy recovery) and stops at the first failed month.
Authentication comes from the Databricks CLI profile (--profile or DATABRICKS_CONFIG_PROFILE); no secrets here.

Examples:
  python scripts/backfill.py --start 2025-01 --end 2025-06 --dry-run
  python scripts/backfill.py --only 2025-02 --inject-failure canary1
  python scripts/backfill.py --start 2025-03 --end 2025-06 --resume
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from landing import build_yellow_url, yellow_filename  # noqa: E402

from taxilake.backfill import check_schema, month_range, pending_months  # noqa: E402

LANDING_DIR = ROOT / "data" / "landing"
LOG_FILE = ROOT / "data" / "backfill_log.jsonl"
PY = sys.executable


def sh(cmd, capture=False, check=True):
    """Run a command from the project root."""
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=capture, check=False)
    if check and result.returncode != 0:
        detail = (result.stdout or "") + (result.stderr or "")
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(map(str, cmd))}\n{detail[-1500:]}")
    return result


class Cli:
    def __init__(self, profile):
        self.profile = ["-p", profile] if profile else []
        self._warehouse = None

    def databricks(self, *args, check=True):
        return sh(["databricks", *args, *self.profile], capture=True, check=check)

    def warehouse_id(self):
        if self._warehouse is None:
            warehouses = json.loads(self.databricks("warehouses", "list", "-o", "json").stdout)
            running = [w for w in warehouses if w.get("enable_serverless_compute")] or warehouses
            if not running:
                raise RuntimeError("no SQL warehouse found")
            self._warehouse = running[0]["id"]
        return self._warehouse

    def sql(self, statement):
        body = {"warehouse_id": self.warehouse_id(), "statement": statement, "wait_timeout": "50s"}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="ascii") as fh:
            json.dump(body, fh)
            path = fh.name
        try:
            out = json.loads(self.databricks("api", "post", "/api/2.0/sql/statements", "--json", f"@{path}").stdout)
        finally:
            os.unlink(path)
        if out["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(f"SQL failed: {out['status'].get('error', out['status'])}")
        return out.get("result", {}).get("data_array", []) or []


def upstream_exists(month):
    request = urllib.request.Request(build_yellow_url(month), method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=30):
            return True
    except Exception:
        return False


def local_parquet(month):
    return LANDING_DIR / "yellow" / month / yellow_filename(month)


def preflight(months):
    """No cloud compute: confirm upstream files exist, download them, check schemas, return {month: source rows}."""
    import pyarrow.parquet as pq

    counts = {}
    for month in months:
        if not upstream_exists(month):
            raise RuntimeError(f"{month}: upstream file not available")
        sh([PY, "scripts/download_sources.py", "--month", month])
        parquet = pq.ParquetFile(local_parquet(month))
        problems = check_schema({f.name: str(f.type) for f in parquet.schema_arrow})
        if problems:
            raise RuntimeError(f"{month}: schema drift against bronze DDL: {problems}")
        counts[month] = parquet.metadata.num_rows
        print(f"preflight {month}: ok, {counts[month]:,} source rows")
    return counts


def reconciled_months(cli):
    rows = cli.sql("SELECT source_month FROM workspace.ops.month_reconciliation WHERE status = 'PASS' GROUP BY 1")
    return {r[0] for r in rows}


def run_month(cli, month, source_rows, inject):
    started = time.time()
    batch_id = "bf" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + month.replace("-", "")
    sh([PY, "scripts/fetch_weather.py", "--month", month])
    upload = [PY, "scripts/upload_to_volume.py", "--month", month]
    if cli.profile:
        upload += ["--profile", cli.profile[1]]
    sh(upload)
    params = f"month={month},batch_id={batch_id}" + (f",inject_failure={inject}" if inject else "")
    result = cli.databricks("bundle", "run", "taxilake_pipeline", "-t", "dev", "--params", params, check=False)
    output = (result.stdout or "") + (result.stderr or "")
    match = re.search(r"runs/(\d+)", output)
    record = {"month": month, "batch_id": batch_id, "inject_failure": inject, "source_rows": source_rows}
    if match:
        run = json.loads(cli.databricks("jobs", "get-run", match.group(1), "-o", "json").stdout)
        record.update(
            run_id=match.group(1),
            result=run["state"].get("result_state"),
            job_ms=run.get("run_duration"),
            tasks=[
                {
                    "task": t["task_key"],
                    "result": t["state"].get("result_state"),
                    "attempt": t.get("attempt_number"),
                    "ms": t.get("execution_duration"),
                }
                for t in run.get("tasks", [])
            ],
        )
    ok = result.returncode == 0 and record.get("result") == "SUCCESS"
    if ok:
        rows = cli.sql(
            "SELECT bronze_rows, status, silver_accepted, quarantined, fact_rows "
            "FROM workspace.ops.month_reconciliation "
            f"WHERE source_month = '{month}' AND run_batch_id = '{batch_id}'"
        )
        if not rows:
            ok, record["error"] = False, "no reconciliation row written"
        else:
            bronze, status = int(rows[0][0]), rows[0][1]
            record.update(
                bronze_rows=bronze,
                status=status,
                accepted=int(rows[0][2]),
                quarantined=int(rows[0][3]),
                fact=int(rows[0][4]),
            )
            if status != "PASS" or bronze != source_rows:
                ok, record["error"] = False, f"status={status}, bronze rows {bronze} vs source {source_rows}"
    else:
        record["error"] = output[-1500:]
    record.update(ok=ok, wall_s=round(time.time() - started, 1))
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2025-01")
    parser.add_argument("--end", default="2025-06")
    parser.add_argument("--only", help="run a single month")
    parser.add_argument("--resume", action="store_true", help="skip months already reconciled (status PASS)")
    parser.add_argument(
        "--inject-failure", default="", help="test id: first silver attempt fails once, the retry passes"
    )
    parser.add_argument("--dry-run", action="store_true", help="pre-flight only; no uploads or cloud runs")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    args = parser.parse_args()

    months = [args.only] if args.only else month_range(args.start, args.end)
    cli = Cli(args.profile)
    todo = pending_months(months, reconciled_months(cli)) if args.resume else months
    print(f"months to run: {todo}")
    if not todo:
        return 0
    counts = preflight(todo)
    if args.dry_run:
        print("dry run complete; nothing was uploaded or run in the cloud")
        return 0

    for month in todo:
        print(f"=== {month} ===")
        record = run_month(cli, month, counts[month], args.inject_failure)
        print(json.dumps({k: v for k, v in record.items() if k != "error"}, indent=None))
        if not record["ok"]:
            print(f"STOPPED at {month}: {record.get('error')}")
            print(
                f"fix the cause, then resume with: python scripts/backfill.py --start {month} --end {todo[-1]} --resume"
            )
            return 1
    print("all requested months reconciled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
