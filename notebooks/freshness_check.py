# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline freshness check
# MAGIC Reads `ops.pipeline_runs` and `ops.month_reconciliation` and reports, per month, when the last complete successful run
# MAGIC finished (all four tasks succeeded in one batch and the reconciliation for that batch is PASS), plus data coverage.
# MAGIC **Freshness here is pipeline freshness, not source-data age**: the input is a fixed historical range, and the newest
# MAGIC month loaded is reported as coverage. Read-only; not part of the pipeline job. Requires `sql/41_ops_pipeline_runs.sql`.
# MAGIC Set `fail_if_not_fresh` to `true` to fail this notebook when any expected month is not FRESH.

# COMMAND ----------

import os
import sys

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(notebook_path)) + "/src")

from taxilake import freshness, namespace  # noqa: E402
from taxilake.backfill import month_range  # noqa: E402

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
dbutils.widgets.text("environment", "dev")  # noqa: F821 - "dev" or "prod"; the job passes it explicitly
dbutils.widgets.text("start_month", "2025-01")  # noqa: F821
dbutils.widgets.text("end_month", "2025-06")  # noqa: F821
dbutils.widgets.text("max_age_hours", str(freshness.DEFAULT_MAX_AGE_HOURS))  # noqa: F821
dbutils.widgets.dropdown("fail_if_not_fresh", "false", ["false", "true"])  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
environment = dbutils.widgets.get("environment")  # noqa: F821
# Resolves catalog + environment to the schemas below; dev can only resolve to `dev_` schemas, never production.
ns = namespace.resolve(catalog, environment)
ops_schema = ns.schema("ops")
expected_months = month_range(dbutils.widgets.get("start_month"), dbutils.widgets.get("end_month"))  # noqa: F821
max_age_hours = float(dbutils.widgets.get("max_age_hours"))  # noqa: F821

# COMMAND ----------

runs = [
    r.asDict()
    for r in spark.table(f"{ops_schema}.pipeline_runs")  # noqa: F821 - spark is provided by Databricks
    .select("source_month", "batch_id", "task_key", "attempt", "status", "finished_at")
    .collect()
]
recon = [
    r.asDict()
    for r in spark.table(f"{ops_schema}.month_reconciliation")  # noqa: F821
    .select("source_month", "run_batch_id", "status")
    .collect()
]
result = freshness.evaluate(runs, recon, expected_months, max_age_hours=max_age_hours)
print(freshness.format_report(result))

if dbutils.widgets.get("fail_if_not_fresh") == "true":  # noqa: F821
    assert freshness.is_healthy(result), f"pipeline is not fresh: overall_state={result['overall_state']}"
