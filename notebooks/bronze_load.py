# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze load
# MAGIC Loads one month of landed files from the volume into the bronze tables with `COPY INTO`.
# MAGIC `COPY INTO` remembers which files it already loaded per target table, so re-running the same month adds zero rows.
# MAGIC Records one row per attempt in `ops.pipeline_runs` (rows read = rows in the landed month file, rows written = rows
# MAGIC `COPY INTO` inserted). Requires `sql/10_bronze_tables.sql`, `sql/41_ops_pipeline_runs.sql` and the files uploaded by
# MAGIC `scripts/upload_to_volume.py`.

# COMMAND ----------

import os
import re
import sys
from datetime import datetime, timezone

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(notebook_path)) + "/src")

from taxilake import namespace, runlog  # noqa: E402

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
dbutils.widgets.text("environment", "dev")  # noqa: F821 - "dev" or "prod"; the job passes it explicitly
dbutils.widgets.text("month", "2025-01")  # noqa: F821
dbutils.widgets.text("batch_id", "")  # noqa: F821
dbutils.widgets.text("job_run_id", "")  # noqa: F821 - optional, filled by the job's parameter reference
dbutils.widgets.text("task_run_id", "")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
environment = dbutils.widgets.get("environment")  # noqa: F821
# Resolves catalog + environment to the schemas below; dev can only resolve to `dev_` schemas, never production.
ns = namespace.resolve(catalog, environment)
bronze_schema = ns.schema("bronze")
ops_schema = ns.schema("ops")
month = dbutils.widgets.get("month")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Values are interpolated into SQL text below, so only accept simple, known shapes.
assert re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month), f"month must be YYYY-MM, got {month!r}"
assert re.fullmatch(r"[A-Za-z0-9_.:-]+", batch_id), f"bad batch_id {batch_id!r}"

run = runlog.TaskRun(
    month,
    batch_id,
    "bronze_load",
    runlog.spark_writer(spark, f"{ops_schema}.pipeline_runs"),  # noqa: F821 - spark is provided by Databricks
    runlog.spark_prior_attempts(spark, f"{ops_schema}.pipeline_runs"),  # noqa: F821
    job_run_id=dbutils.widgets.get("job_run_id"),  # noqa: F821
    task_run_id=dbutils.widgets.get("task_run_id"),  # noqa: F821
)

root = ns.volume_root
print(f"catalog={catalog} environment={environment} month={month} batch_id={batch_id}")

META = f"_metadata.file_path AS _source_file, current_timestamp() AS _ingested_at, '{batch_id}' AS _batch_id"

# COMMAND ----------


def copy_into(table: str, source_dir: str, fmt: str, pattern: str, options: str = "", columns: str = "*") -> dict:
    before = spark.table(f"{bronze_schema}.{table}").count()  # noqa: F821
    result = spark.sql(  # noqa: F821
        f"""
        COPY INTO {bronze_schema}.{table}
        FROM (SELECT {columns}, {META} FROM '{source_dir}')
        FILEFORMAT = {fmt}
        PATTERN = '{pattern}'
        {options}
        """
    ).collect()[0]
    after = spark.table(f"{bronze_schema}.{table}").count()  # noqa: F821
    inserted = int(result["num_inserted_rows"])
    print(f"{table}: before={before} inserted_by_copy_into={inserted} after={after}")
    return {"inserted": inserted, "after": after}


# COMMAND ----------

with run.guard():
    # Rows in the landed month file (a Parquet footer read), the "rows read" of this task.
    run.note(rows_read=spark.read.parquet(f"{root}/yellow/{month}/yellow_tripdata_{month}.parquet").count())  # noqa: F821

    yellow = copy_into(
        "yellow_trips",
        f"{root}/yellow/{month}",
        "PARQUET",
        f"yellow_tripdata_{month}.parquet",
    )
    zones = copy_into(
        "taxi_zones",
        f"{root}/zones",
        "CSV",
        "taxi_zone_lookup.csv",
        "FORMAT_OPTIONS ('header' = 'true')",
        # CSV columns arrive as strings; the only cast is LocationID to match the explicit table schema.
        columns="CAST(LocationID AS INT) AS LocationID, Borough, Zone, service_zone",
    )
    weather = copy_into(
        "weather_hourly",
        f"{root}/weather/{month}",
        "JSON",
        f"weather_{month}.jsonl",
    )

    run.succeed(
        rows_written=yellow["inserted"],
        details={
            "yellow_table_rows_after": yellow["after"],
            "zones_inserted": zones["inserted"],
            "weather_inserted": weather["inserted"],
        },
    )
