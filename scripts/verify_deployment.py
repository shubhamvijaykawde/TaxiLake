"""Post-deploy safety gate for the production bundle.

Reads JSON files saved from the Databricks CLI (so nothing host- or user-specific is ever printed) and fails unless:
  * the bundle contains exactly one job, `taxilake_pipeline`;
  * its schedule is PAUSED both in the bundle summary and in the deployed job;
  * no run was started by the schedule (and, with --require-no-runs, no run exists at all).
Only PASS/FAIL lines are printed. CI never starts a job, so a run can only come from someone else.

Usage (CI):
  databricks bundle summary -t prod -o json > summary.json
  databricks jobs get <id> -o json > job.json
  databricks jobs list-runs --job-id <id> -o json > runs.json
  python scripts/verify_deployment.py --summary summary.json --job job.json --runs runs.json
"""

import argparse
import json
import sys
from pathlib import Path

EXPECTED_JOB = "taxilake_pipeline"


def load_json(path: str):
    """Load a CLI JSON file; an empty file (e.g. `list-runs` with no runs) means an empty list."""
    text = Path(path).read_text(encoding="utf-8-sig").strip()
    return json.loads(text) if text else []


def _run_list(runs) -> list:
    if isinstance(runs, dict):
        return runs.get("runs", []) or []
    return runs or []


def check(summary: dict, job: dict, runs, require_no_runs: bool = False) -> list:
    """Return a list of problems; empty means the deployment is safe."""
    problems = []
    jobs = (summary.get("resources") or {}).get("jobs") or {}
    if set(jobs) != {EXPECTED_JOB}:
        problems.append(f"bundle must contain exactly one job named {EXPECTED_JOB}, found {sorted(jobs)}")
    summary_status = ((jobs.get(EXPECTED_JOB) or {}).get("schedule") or {}).get("pause_status")
    if summary_status != "PAUSED":
        problems.append(f"bundle summary schedule pause_status is {summary_status!r}, expected 'PAUSED'")
    deployed_status = (((job.get("settings") or {}).get("schedule")) or {}).get("pause_status")
    if deployed_status != "PAUSED":
        problems.append(f"deployed job schedule pause_status is {deployed_status!r}, expected 'PAUSED'")
    run_list = _run_list(runs)
    if require_no_runs and run_list:
        problems.append(f"job has {len(run_list)} run(s); expected none")
    scheduled = [r for r in run_list if r.get("trigger") == "PERIODIC"]
    if scheduled:
        problems.append(f"{len(scheduled)} run(s) were started by the schedule")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--runs", required=True)
    parser.add_argument("--require-no-runs", action="store_true")
    args = parser.parse_args()
    problems = check(load_json(args.summary), load_json(args.job), load_json(args.runs), args.require_no_runs)
    if problems:
        for problem in problems:
            print("FAIL:", problem)
        return 1
    print("PASS: only taxilake_pipeline is deployed, its schedule is PAUSED, and no scheduled run exists")
    return 0


if __name__ == "__main__":
    sys.exit(main())
