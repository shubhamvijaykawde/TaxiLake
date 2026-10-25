# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transform
# MAGIC Reads one month of `bronze.yellow_trips`, applies `taxilake.silver.transform_bronze`, and replaces that month's slice of
# MAGIC `silver.trips` and `silver.trips_quarantine` (Delta `replaceWhere`), so re-runs are idempotent.
# MAGIC Asserts `bronze rows = accepted + quarantined + duplicates removed`. Requires `sql/20_silver_tables.sql`.

# COMMAND ----------

import os
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

# The bundle deploys the whole project; put its `src/` folder on the path so the package imports like it does in tests.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(notebook_path)) + "/src")

from taxilake import silver  # noqa: E402
from taxilake.injection import should_fail  # noqa: E402

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
dbutils.widgets.text("month", "2025-01")  # noqa: F821
dbutils.widgets.text("batch_id", "")  # noqa: F821
dbutils.widgets.text("inject_failure", "")  # noqa: F821 - test id; empty (default) disables injection

catalog = dbutils.widgets.get("catalog")  # noqa: F821
month = dbutils.widgets.get("month")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

assert re.fullmatch(r"[A-Za-z0-9_]+", catalog), f"bad catalog {catalog!r}"
assert re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month), f"month must be YYYY-MM, got {month!r}"
assert re.fullmatch(r"[A-Za-z0-9_.:-]+", batch_id), f"bad batch_id {batch_id!r}"
inject_failure = dbutils.widgets.get("inject_failure")  # noqa: F821
assert re.fullmatch(r"[A-Za-z0-9_.:-]*", inject_failure), f"bad inject_failure {inject_failure!r}"
print(f"catalog={catalog} month={month} batch_id={batch_id}")

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.yellow_trips").where(  # noqa: F821 - spark is provided by Databricks
    F.col("_source_file").endswith(f"yellow_tripdata_{month}.parquet")
)
bronze_rows = bronze.count()
assert bronze_rows > 0, f"no bronze rows for {month}; run the bronze load first"

# Controlled failure test: the first attempt leaves a marker row and fails; the retry sees the marker and proceeds.
if inject_failure:
    marker_exists = (
        spark.table(f"{catalog}.ops.failure_injection").where(F.col("test_id") == inject_failure).count() > 0  # noqa: F821
    )
    if should_fail(inject_failure, marker_exists):
        spark.sql(  # noqa: F821
            f"INSERT INTO {catalog}.ops.failure_injection VALUES ('{inject_failure}', current_timestamp())"
        )
        raise RuntimeError(f"injected failure for test {inject_failure!r} (first attempt)")
    print(f"failure injection {inject_failure!r}: marker found, retry proceeding")

valid, quarantine, duplicates_removed = silver.transform_bronze(bronze)
# Silver lineage: the run's batch id replaces the bronze batch id so each silver slice is traceable to this run.
valid = valid.withColumn("_batch_id", F.lit(batch_id))
quarantine = quarantine.withColumn("_batch_id", F.lit(batch_id))

TRIP_COLUMNS = silver.SOURCE_COLUMNS + [
    "trip_duration_min",
    "source_month",
    "_source_file",
    "_batch_id",
    "_processed_at",
]
slice_filter = f"source_month = '{month}'"


def replace_slice(df, table: str, columns: list) -> None:
    (
        df.select(*columns)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", slice_filter)
        .saveAsTable(f"{catalog}.silver.{table}")
    )


replace_slice(valid, "trips", TRIP_COLUMNS)
replace_slice(quarantine, "trips_quarantine", TRIP_COLUMNS + ["reason_codes"])

# COMMAND ----------

# Reconcile from the tables that were just written, not from the in-memory frames.
accepted = spark.table(f"{catalog}.silver.trips").where(slice_filter).count()  # noqa: F821
quarantined = spark.table(f"{catalog}.silver.trips_quarantine").where(slice_filter).count()  # noqa: F821
print(
    f"bronze_rows={bronze_rows} accepted={accepted} quarantined={quarantined} "
    f"duplicates_removed={duplicates_removed} sum={accepted + quarantined + duplicates_removed}"
)
assert accepted + quarantined + duplicates_removed == bronze_rows, "reconciliation failed"

breakdown = (
    spark.table(f"{catalog}.silver.trips_quarantine")  # noqa: F821
    .where(slice_filter)
    .select(F.explode("reason_codes").alias("reason_code"))
    .groupBy("reason_code")
    .count()
    .orderBy("reason_code")
    .collect()
)
for row in breakdown:
    print(f"reason {row['reason_code']}: {row['count']}")
print("RECONCILIATION OK")
