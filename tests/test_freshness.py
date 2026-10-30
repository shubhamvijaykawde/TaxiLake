from datetime import datetime, timedelta, timezone

import pytest

from taxilake import freshness as fr
from taxilake.runlog import TASK_KEYS

NOW = datetime(2026, 3, 1, tzinfo=timezone.utc)


def at(hours_ago):
    return NOW - timedelta(hours=hours_ago)


def task_rows(month, batch, hours_ago=10, statuses=None, attempt=1):
    """One row per task; statuses maps task_key -> status (default SUCCESS); later tasks finish later."""
    statuses = statuses or {}
    return [
        {
            "source_month": month,
            "batch_id": batch,
            "task_key": task,
            "attempt": attempt,
            "status": statuses.get(task, "SUCCESS"),
            "finished_at": at(hours_ago - i),
        }
        for i, task in enumerate(TASK_KEYS)
    ]


def recon(month, batch, status="PASS"):
    return {"source_month": month, "run_batch_id": batch, "status": status}


def state_of(result, month):
    return next(m for m in result["months"] if m["month"] == month)


def test_all_months_fresh():
    runs = task_rows("2025-01", "a") + task_rows("2025-02", "b")
    result = fr.evaluate(runs, [recon("2025-01", "a"), recon("2025-02", "b")], ["2025-01", "2025-02"], NOW)
    assert result["overall_state"] == fr.FRESH and fr.is_healthy(result)
    assert result["coverage"]["months_without_successful_run"] == []
    assert result["coverage"]["newest_month_loaded"] == "2025-02"


def test_age_is_measured_from_the_last_task_of_the_complete_run():
    result = fr.evaluate(task_rows("2025-01", "a", hours_ago=10), [recon("2025-01", "a")], ["2025-01"], NOW)
    assert state_of(result, "2025-01")["age_hours"] == 7.0  # the last task finished 10 - 3 hours ago


def test_stale_when_older_than_the_threshold_and_boundary_is_inclusive():
    runs = task_rows("2025-01", "a", hours_ago=103)  # last task finished 100 h ago
    old = fr.evaluate(runs, [recon("2025-01", "a")], ["2025-01"], NOW, max_age_hours=99)
    edge = fr.evaluate(runs, [recon("2025-01", "a")], ["2025-01"], NOW, max_age_hours=100)
    assert old["overall_state"] == fr.STALE and not fr.is_healthy(old)
    assert edge["overall_state"] == fr.FRESH


def test_never_run_month_and_coverage_gap():
    result = fr.evaluate(task_rows("2025-01", "a"), [recon("2025-01", "a")], ["2025-01", "2025-02"], NOW)
    assert state_of(result, "2025-02")["state"] == fr.NEVER_RUN
    assert result["overall_state"] == fr.NEVER_RUN
    assert result["coverage"]["months_without_successful_run"] == ["2025-02"]


def test_failed_last_run_after_an_earlier_success_reports_both():
    runs = task_rows("2025-01", "old", hours_ago=500) + task_rows(
        "2025-01", "new", hours_ago=10, statuses={"silver_transform": "FAILED"}
    )
    result = fr.evaluate(runs, [recon("2025-01", "old")], ["2025-01"], NOW)
    month = state_of(result, "2025-01")
    assert month["state"] == fr.FAILED_LAST_RUN
    assert month["last_success_at"] == at(500 - 3)  # the earlier complete run is still reported
    assert result["overall_state"] == fr.FAILED_LAST_RUN


def test_retry_that_succeeds_within_the_same_batch_is_complete():
    first = task_rows("2025-01", "a", hours_ago=10, statuses={"silver_transform": "FAILED"})
    retry = [
        {**r, "attempt": 2, "status": "SUCCESS", "finished_at": at(5)}
        for r in first
        if r["task_key"] == "silver_transform"
    ]
    result = fr.evaluate(first + retry, [recon("2025-01", "a")], ["2025-01"], NOW)
    assert state_of(result, "2025-01")["state"] == fr.FRESH


def test_rerun_after_failure_is_fresh_again():
    failed = task_rows("2025-01", "a", hours_ago=50, statuses={"gold_build": "FAILED"})
    rerun = task_rows("2025-01", "b", hours_ago=5)
    result = fr.evaluate(failed + rerun, [recon("2025-01", "b")], ["2025-01"], NOW)
    assert state_of(result, "2025-01")["state"] == fr.FRESH


def test_tasks_from_different_batches_do_not_make_a_complete_run():
    mixed = task_rows("2025-01", "a")[:2] + task_rows("2025-01", "b")[2:]
    result = fr.evaluate(mixed, [recon("2025-01", "a"), recon("2025-01", "b")], ["2025-01"], NOW)
    assert state_of(result, "2025-01")["state"] == fr.INCOMPLETE_LAST_RUN
    assert result["coverage"]["months_without_successful_run"] == ["2025-01"]


def test_reconciliation_that_did_not_pass_blocks_completeness():
    for rows in ([recon("2025-01", "a", "FAIL")], [], [recon("2025-01", "other")]):
        result = fr.evaluate(task_rows("2025-01", "a"), rows, ["2025-01"], NOW)
        assert result["overall_state"] == fr.INCOMPLETE_LAST_RUN, rows


def test_empty_input_is_never_run_and_overall_is_never_run():
    result = fr.evaluate([], [], ["2025-01"], NOW)
    assert result["overall_state"] == fr.NEVER_RUN and result["newest_success_at"] is None
    assert result["coverage"]["newest_month_loaded"] is None


def test_overall_state_is_the_worst_of_the_months():
    runs = task_rows("2025-01", "a") + task_rows("2025-02", "b", hours_ago=2000)
    result = fr.evaluate(runs, [recon("2025-01", "a"), recon("2025-02", "b")], ["2025-01", "2025-02"], NOW, 100)
    assert result["overall_state"] == fr.STALE


def test_naive_timestamps_are_treated_as_utc():
    rows = [{**r, "finished_at": r["finished_at"].replace(tzinfo=None)} for r in task_rows("2025-01", "a")]
    result = fr.evaluate(rows, [recon("2025-01", "a")], ["2025-01"], NOW)
    assert result["overall_state"] == fr.FRESH


def test_months_outside_the_expected_range_do_not_change_overall_state():
    runs = task_rows("2025-01", "a") + task_rows("2024-12", "x", hours_ago=5000)
    result = fr.evaluate(runs, [recon("2025-01", "a"), recon("2024-12", "x")], ["2025-01"], NOW, 100 * 24)
    assert result["overall_state"] == fr.FRESH


def test_report_says_freshness_is_not_source_age():
    result = fr.evaluate(task_rows("2025-01", "a"), [recon("2025-01", "a")], ["2025-01"], NOW)
    report = fr.format_report(result)
    assert "not source data age" in report and "newest_month_loaded=2025-01" in report


@pytest.mark.parametrize("state", fr.SEVERITY)
def test_every_state_has_a_severity(state):
    assert state in fr.SEVERITY
