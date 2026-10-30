"""Per-task run logging for `ops.pipeline_runs` (pure logic; Spark is touched only by the two small helpers at the end).

One row is written per task attempt. The success row is written only after all data work and assertions have finished,
and a failed write of that row fails the task, so a successful transformation is never reported without its evidence
(the retry is safe because every task is idempotent). A failed task logs a FAILED row on a best-effort basis; if that
write also fails a warning is printed and the original error still propagates.
"""

import json
import re
from contextlib import contextmanager
from datetime import datetime, timezone

TASK_KEYS = ("bronze_load", "silver_transform", "gold_build", "checks")
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
COLUMNS = [
    "source_month",
    "batch_id",
    "task_key",
    "attempt",
    "job_run_id",
    "task_run_id",
    "started_at",
    "finished_at",
    "duration_s",
    "status",
    "rows_read",
    "rows_written",
    "rows_quarantined",
    "details",
    "error_message",
]
COUNT_FIELDS = ("rows_read", "rows_written", "rows_quarantined")
MAX_ERROR_CHARS = 2000
_RUN_ID_RE = re.compile(r"\d{1,20}")


class RunLogError(RuntimeError):
    """The run record could not be written; raised so the task does not report success without evidence."""


def clean_run_id(value):
    """Return a numeric Databricks run id as a string, or None when it is empty or not a plain number.

    A job parameter reference that Databricks did not resolve arrives as literal text such as '{{job.run_id}}';
    that must not fail a data task, so it is treated as unknown.
    """
    text = (value or "").strip()
    return text if _RUN_ID_RE.fullmatch(text) else None


def balance_problems(task_key, rows_read, rows_written, rows_quarantined, details):
    """Count identities that must hold for a successful task; returns a list of problems (empty means consistent)."""
    problems = []
    if task_key == "silver_transform":
        duplicates = (details or {}).get("duplicates_removed")
        if None in (rows_read, rows_written, rows_quarantined, duplicates):
            problems.append(
                "silver_transform must record rows_read, rows_written, rows_quarantined and duplicates_removed"
            )
        elif rows_written + rows_quarantined + duplicates != rows_read:
            problems.append(
                f"rows_written {rows_written} + rows_quarantined {rows_quarantined} + duplicates_removed {duplicates} "
                f"!= rows_read {rows_read}"
            )
    return problems


def build_row(
    *, month, batch_id, task_key, attempt, job_run_id, task_run_id, started_at, finished_at, status, metrics, error=None
) -> dict:
    """Assemble one pipeline_runs row (keys equal COLUMNS). Counts are non-negative ints or None."""
    if task_key not in TASK_KEYS:
        raise ValueError(f"unknown task_key {task_key!r}")
    if status not in (STATUS_SUCCESS, STATUS_FAILED):
        raise ValueError(f"unknown status {status!r}")
    counts = {}
    for name in COUNT_FIELDS:
        value = metrics.get(name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"{name} must be a non-negative integer or None, got {value!r}")
        counts[name] = value
    details = metrics.get("details")
    return {
        "source_month": month,
        "batch_id": batch_id,
        "task_key": task_key,
        "attempt": attempt,
        "job_run_id": job_run_id,
        "task_run_id": task_run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": round((finished_at - started_at).total_seconds(), 3),
        "status": status,
        **counts,
        "details": json.dumps(details, sort_keys=True, default=str) if details else None,
        "error_message": (error[:MAX_ERROR_CHARS] if error else None),
    }


def _utc_now():
    return datetime.now(timezone.utc)


class TaskRun:
    """Records one task attempt. Use `guard()` around each unit of work and `succeed()` as the last statement.

    write(row) appends the row; prior_attempts(month, batch_id, task_key) returns how many rows already exist for this
    task in this batch (so a retry, which reuses the batch id, gets the next attempt number).
    """

    def __init__(
        self, month, batch_id, task_key, write, prior_attempts, job_run_id="", task_run_id="", clock=_utc_now, log=print
    ):
        if task_key not in TASK_KEYS:
            raise ValueError(f"unknown task_key {task_key!r}")
        self.month, self.batch_id, self.task_key = month, batch_id, task_key
        self.job_run_id, self.task_run_id = clean_run_id(job_run_id), clean_run_id(task_run_id)
        for raw, cleaned, name in (
            (job_run_id, self.job_run_id, "job_run_id"),
            (task_run_id, self.task_run_id, "task_run_id"),
        ):
            if raw and cleaned is None:
                log(f"WARNING: {name} {raw!r} is not a number; recording it as unknown")
        self._write, self._prior_attempts, self._clock, self._log = write, prior_attempts, clock, log
        self.started_at = clock()
        self.metrics = {}
        self.done = False

    def note(self, **metrics) -> None:
        """Remember counts known so far, so a later failure still records how far the task got."""
        self.metrics.update(metrics)

    def _record(self, status, error=None):
        row = build_row(
            month=self.month,
            batch_id=self.batch_id,
            task_key=self.task_key,
            attempt=self._prior_attempts(self.month, self.batch_id, self.task_key) + 1,
            job_run_id=self.job_run_id,
            task_run_id=self.task_run_id,
            started_at=self.started_at,
            finished_at=self._clock(),
            status=status,
            metrics=self.metrics,
            error=error,
        )
        self._write(row)
        return row

    def succeed(self, **metrics) -> dict:
        """Write the SUCCESS row. Raises (without writing SUCCESS) if the counts do not balance or the write fails."""
        self.note(**metrics)
        problems = balance_problems(
            self.task_key,
            self.metrics.get("rows_read"),
            self.metrics.get("rows_written"),
            self.metrics.get("rows_quarantined"),
            self.metrics.get("details"),
        )
        if problems:
            raise AssertionError("count balance failed: " + "; ".join(problems))
        try:
            row = self._record(STATUS_SUCCESS)
        except Exception as exc:
            raise RunLogError(f"could not write the {self.task_key} run record: {exc}") from exc
        self.done = True
        return row

    @contextmanager
    def guard(self):
        """Log a FAILED row (best effort) for any exception, then let the original exception propagate."""
        try:
            yield self
        except BaseException as exc:
            if not self.done:
                self.done = True
                try:
                    self._record(STATUS_FAILED, error=f"{type(exc).__name__}: {exc}")
                except Exception as log_exc:  # never mask the real failure
                    self._log(f"WARNING: could not write the FAILED run record ({type(log_exc).__name__}: {log_exc})")
            raise


def spark_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("source_month", T.StringType()),
            T.StructField("batch_id", T.StringType()),
            T.StructField("task_key", T.StringType()),
            T.StructField("attempt", T.IntegerType()),
            T.StructField("job_run_id", T.StringType()),
            T.StructField("task_run_id", T.StringType()),
            T.StructField("started_at", T.TimestampType()),
            T.StructField("finished_at", T.TimestampType()),
            T.StructField("duration_s", T.DoubleType()),
            T.StructField("status", T.StringType()),
            T.StructField("rows_read", T.LongType()),
            T.StructField("rows_written", T.LongType()),
            T.StructField("rows_quarantined", T.LongType()),
            T.StructField("details", T.StringType()),
            T.StructField("error_message", T.StringType()),
        ]
    )


def spark_writer(spark, table):
    """Return write(row) that appends one row to `table` (the environment's fully qualified ops.pipeline_runs)."""

    def write(row):
        spark.createDataFrame([tuple(row[c] for c in COLUMNS)], schema=spark_schema()).write.format("delta").mode(
            "append"
        ).saveAsTable(table)

    return write


def spark_prior_attempts(spark, table):
    """Return prior_attempts(month, batch_id, task_key): existing rows for this task in this batch."""

    def prior(month, batch_id, task_key):
        from pyspark.sql import functions as F

        return (
            spark.table(table)
            .where((F.col("source_month") == month) & (F.col("batch_id") == batch_id) & (F.col("task_key") == task_key))
            .count()
        )

    return prior
