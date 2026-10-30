"""Pipeline freshness and data coverage from `ops.pipeline_runs` and `ops.month_reconciliation` (pure logic).

Freshness here means PIPELINE freshness: how long ago a month was last processed by a complete, successful run. It says
nothing about the age of the source data. NYC TLC and Open-Meteo history is published with a lag and this project loads
a fixed historical range, so "newest source month loaded" is reported separately as coverage and is never called fresh.

A month has a complete successful run for a batch when every task in TASK_KEYS has, as its latest attempt in that batch,
a SUCCESS row, and `ops.month_reconciliation` holds a PASS row for the same month and batch.
"""

from datetime import datetime, timezone

from taxilake.runlog import STATUS_FAILED, STATUS_SUCCESS, TASK_KEYS

DEFAULT_MAX_AGE_HOURS = 35 * 24  # a monthly cadence plus a few days of slack

FRESH = "FRESH"
STALE = "STALE"
FAILED_LAST_RUN = "FAILED_LAST_RUN"
INCOMPLETE_LAST_RUN = "INCOMPLETE_LAST_RUN"
NEVER_RUN = "NEVER_RUN"
# Worst first: the overall state is the worst state among the expected months.
SEVERITY = [FAILED_LAST_RUN, NEVER_RUN, INCOMPLETE_LAST_RUN, STALE, FRESH]

SOURCE_NOTE = (
    "Freshness is about pipeline runs, not source data age. The input is historical NYC TLC and weather data for a "
    "fixed range; 'newest_month_loaded' is coverage, not freshness."
)


def _aware(value):
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _latest_attempts(rows):
    """Latest attempt per task within one batch (highest attempt, then latest finished_at)."""
    latest = {}
    for row in rows:
        key = row["task_key"]
        rank = (row.get("attempt") or 0, _aware(row["finished_at"]))
        if key not in latest or rank > latest[key][0]:
            latest[key] = (rank, row)
    return {key: value[1] for key, value in latest.items()}


def _batch_summary(rows, recon_pass):
    """Return (complete, failed, completed_at) for the rows of one month and batch."""
    latest = _latest_attempts(rows)
    failed = any(row["status"] == STATUS_FAILED for row in latest.values())
    complete = all(task in latest and latest[task]["status"] == STATUS_SUCCESS for task in TASK_KEYS) and recon_pass
    finished = [_aware(row["finished_at"]) for row in latest.values()]
    return complete, failed, (max(finished) if finished else None)


def evaluate_month(month, runs, recon, now, max_age_hours):
    """State of one month. runs: pipeline_runs dicts; recon: month_reconciliation dicts (both for this month only)."""
    if not runs:
        return {
            "month": month,
            "state": NEVER_RUN,
            "last_success_at": None,
            "age_hours": None,
            "last_batch_id": None,
            "detail": "no pipeline_runs rows",
        }
    passing = {r["run_batch_id"] for r in recon if r["status"] == "PASS"}
    batches = {}
    for row in runs:
        batches.setdefault(row["batch_id"], []).append(row)
    summaries = {}
    for batch_id, rows in batches.items():
        complete, failed, completed_at = _batch_summary(rows, batch_id in passing)
        last_activity = max(_aware(r["finished_at"]) for r in rows)
        summaries[batch_id] = (complete, failed, completed_at, last_activity)

    last_batch = max(summaries, key=lambda b: summaries[b][3])
    complete_batches = [b for b, s in summaries.items() if s[0]]
    last_success_at = max((summaries[b][2] for b in complete_batches), default=None)
    complete, failed, completed_at, _ = summaries[last_batch]
    base = {"month": month, "last_success_at": last_success_at, "last_batch_id": last_batch}
    if complete:
        age = (now - completed_at).total_seconds() / 3600
        state = FRESH if age <= max_age_hours else STALE
        return {**base, "state": state, "age_hours": round(age, 2), "detail": "last run complete"}
    age = round((now - last_success_at).total_seconds() / 3600, 2) if last_success_at else None
    if failed:
        return {**base, "state": FAILED_LAST_RUN, "age_hours": age, "detail": "a task failed in the latest batch"}
    return {
        **base,
        "state": INCOMPLETE_LAST_RUN,
        "age_hours": age,
        "detail": "latest batch lacks a successful task or a PASS reconciliation",
    }


def evaluate(runs, recon, expected_months, now=None, max_age_hours=DEFAULT_MAX_AGE_HOURS) -> dict:
    """Freshness per expected month, the overall (worst) state and data coverage.

    Months outside `expected_months` still count towards `newest_month_loaded`, but not towards the overall state.
    """
    now = _aware(now) if now else datetime.now(timezone.utc)
    months_seen = {r["source_month"] for r in runs}
    per_month = []
    for month in sorted(set(expected_months) | months_seen):
        month_runs = [r for r in runs if r["source_month"] == month]
        month_recon = [r for r in recon if r["source_month"] == month]
        per_month.append(evaluate_month(month, month_runs, month_recon, now, max_age_hours))
    expected = [m for m in per_month if m["month"] in set(expected_months)]
    overall = None
    for state in SEVERITY:
        if any(m["state"] == state for m in expected):
            overall = state
            break
    loaded = sorted(m["month"] for m in per_month if m["last_success_at"] is not None)
    successes = [m["last_success_at"] for m in expected if m["last_success_at"] is not None]
    return {
        "overall_state": overall or NEVER_RUN,
        "max_age_hours": max_age_hours,
        "newest_success_at": max(successes) if successes else None,
        "months": per_month,
        "coverage": {
            "expected_months": sorted(expected_months),
            "months_with_successful_run": [m for m in loaded if m in set(expected_months)],
            "months_without_successful_run": [m for m in sorted(expected_months) if m not in loaded],
            "newest_month_loaded": loaded[-1] if loaded else None,
        },
        "note": SOURCE_NOTE,
    }


def is_healthy(result) -> bool:
    """True only when every expected month is FRESH."""
    return result["overall_state"] == FRESH


def format_report(result) -> str:
    lines = [f"overall_state={result['overall_state']} (threshold {result['max_age_hours']} h)"]
    for m in result["months"]:
        age = "n/a" if m["age_hours"] is None else f"{m['age_hours']} h"
        lines.append(f"  {m['month']}: {m['state']} last_success_at={m['last_success_at']} age={age} ({m['detail']})")
    cov = result["coverage"]
    lines.append(
        f"coverage: with successful run={cov['months_with_successful_run']} "
        f"without={cov['months_without_successful_run']} newest_month_loaded={cov['newest_month_loaded']}"
    )
    lines.append(result["note"])
    return "\n".join(lines)
