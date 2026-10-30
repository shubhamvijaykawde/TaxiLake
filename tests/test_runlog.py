import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from taxilake import runlog
from taxilake.runlog import RunLogError, TaskRun, balance_problems, build_row, clean_run_id

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.now = T0

    def __call__(self):
        current = self.now
        self.now = self.now + timedelta(seconds=10)
        return current


def make_run(task_key="silver_transform", prior=0, write=None, **kwargs):
    rows = []
    run = TaskRun(
        "2025-01",
        "b1",
        task_key,
        write or rows.append,
        lambda month, batch, task: prior,
        clock=Clock(),
        log=kwargs.pop("log", lambda message: None),
        **kwargs,
    )
    return run, rows


SILVER = {"rows_read": 100, "rows_written": 90, "rows_quarantined": 8, "details": {"duplicates_removed": 2}}


def test_success_row_has_counts_timings_and_nulls_for_not_applicable():
    run, rows = make_run("gold_build", job_run_id="123", task_run_id="456")
    with run.guard():
        run.succeed(rows_read=5, rows_written=5, details={"inserted": 5, "updated": 0})
    (row,) = rows
    assert list(row) == runlog.COLUMNS
    assert (row["status"], row["attempt"], row["job_run_id"], row["task_run_id"]) == ("SUCCESS", 1, "123", "456")
    assert row["duration_s"] == 10.0
    assert row["rows_quarantined"] is None  # not applicable, not zero
    assert row["details"] == '{"inserted": 5, "updated": 0}'
    assert row["error_message"] is None


def test_zero_is_kept_distinct_from_none():
    run, rows = make_run("gold_build")
    with run.guard():
        run.succeed(rows_read=0, rows_written=0)
    assert rows[0]["rows_read"] == 0 and rows[0]["rows_quarantined"] is None


def test_retry_gets_the_next_attempt_number():
    run, rows = make_run("gold_build", prior=1)
    with run.guard():
        run.succeed(rows_read=1, rows_written=1)
    assert rows[0]["attempt"] == 2


def test_failed_task_logs_failed_row_with_partial_counts_and_reraises_the_same_error():
    run, rows = make_run()
    error = RuntimeError("injected failure")
    with pytest.raises(RuntimeError) as caught:
        with run.guard():
            run.note(rows_read=100)
            raise error
    assert caught.value is error
    (row,) = rows
    assert row["status"] == "FAILED" and row["rows_read"] == 100 and row["rows_written"] is None
    assert row["error_message"] == "RuntimeError: injected failure"


def test_failed_row_is_written_once_even_if_several_guards_see_the_error():
    run, rows = make_run()
    with pytest.raises(ValueError):
        with run.guard():
            with run.guard():
                raise ValueError("boom")
    assert len(rows) == 1


def test_error_after_success_row_does_not_add_a_failed_row():
    run, rows = make_run("gold_build")
    with pytest.raises(ValueError):
        with run.guard():
            run.succeed(rows_read=1, rows_written=1)
            raise ValueError("after the record")
    assert [r["status"] for r in rows] == ["SUCCESS"]


def test_log_write_failure_on_success_path_fails_the_task():
    def broken(row):
        raise OSError("table missing")

    run, _ = make_run("gold_build", write=broken)
    with pytest.raises(RunLogError, match="could not write the gold_build run record"):
        with run.guard():
            run.succeed(rows_read=1, rows_written=1)


def test_log_write_failure_on_failure_path_does_not_mask_the_original_error():
    messages = []

    def broken(row):
        raise OSError("table missing")

    run, _ = make_run(write=broken, log=messages.append)
    with pytest.raises(RuntimeError, match="real failure"):
        with run.guard():
            raise RuntimeError("real failure")
    assert any("could not write the FAILED run record" in m for m in messages)


def test_silver_counts_must_balance_before_success_is_recorded():
    run, rows = make_run()
    with pytest.raises(AssertionError, match="count balance failed"):
        with run.guard():
            run.succeed(rows_read=100, rows_written=90, rows_quarantined=8, details={"duplicates_removed": 1})
    assert [r["status"] for r in rows] == ["FAILED"]  # the failure is recorded, never a SUCCESS


def test_balanced_silver_counts_succeed():
    run, rows = make_run()
    with run.guard():
        run.succeed(**SILVER)
    assert rows[0]["status"] == "SUCCESS" and rows[0]["rows_quarantined"] == 8


def test_balance_problems_cases():
    assert balance_problems("silver_transform", 100, 90, 8, {"duplicates_removed": 2}) == []
    assert balance_problems("silver_transform", 100, 90, 8, {"duplicates_removed": 3})
    assert balance_problems("silver_transform", 100, 90, None, {"duplicates_removed": 2})
    assert balance_problems("gold_build", 1, 2, None, None) == []


@pytest.mark.parametrize(
    "value,expected", [("123", "123"), ("", None), (None, None), ("{{job.run_id}}", None), ("12a", None), (" 7 ", "7")]
)
def test_clean_run_id(value, expected):
    assert clean_run_id(value) == expected


def test_unresolved_run_id_is_recorded_as_unknown_with_a_warning():
    messages = []
    run, rows = make_run("gold_build", job_run_id="{{job.run_id}}", log=messages.append)
    with run.guard():
        run.succeed(rows_read=1, rows_written=1)
    assert rows[0]["job_run_id"] is None and any("job_run_id" in m for m in messages)


@pytest.mark.parametrize("bad", [-1, 1.5, "3", True])
def test_build_row_rejects_invalid_counts(bad):
    kwargs = dict(
        month="2025-01", batch_id="b", task_key="checks", attempt=1, job_run_id=None, task_run_id=None,
        started_at=T0, finished_at=T0, status="SUCCESS",
    )  # fmt: skip
    with pytest.raises(ValueError):
        build_row(metrics={"rows_read": bad}, **kwargs)


def test_unknown_task_key_is_rejected():
    with pytest.raises(ValueError):
        TaskRun("2025-01", "b", "not_a_task", lambda r: None, lambda *a: 0)


def test_row_columns_match_the_ddl_and_the_spark_schema(spark):
    ddl = (ROOT / "sql" / "41_ops_pipeline_runs.sql").read_text(encoding="utf-8")
    body = ddl.split("pipeline_runs (", 1)[1].split(") COMMENT", 1)[0]
    ddl_columns = re.findall(r"^\s+(\w+)\s+[A-Z]+", body, re.M)
    assert ddl_columns == runlog.COLUMNS
    assert [f.name for f in runlog.spark_schema().fields] == runlog.COLUMNS


def test_a_real_row_fits_the_spark_schema(spark):
    run, rows = make_run()
    with run.guard():
        run.succeed(**SILVER)
    frame = spark.createDataFrame([tuple(rows[0][c] for c in runlog.COLUMNS)], schema=runlog.spark_schema())
    assert frame.count() == 1 and frame.first()["rows_quarantined"] == 8
