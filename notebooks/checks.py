# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-layer checks
# MAGIC Reconciles one month across bronze, silver and gold (`taxilake.reconcile.evaluate`), records a fingerprint of the gold
# MAGIC slice, appends one row to `ops.month_reconciliation`, and fails the task if anything does not reconcile.
# MAGIC Requires `sql/40_ops_tables.sql`.

# COMMAND ----------

import os
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(notebook_path)) + "/src")

from taxilake import gold, namespace, reconcile, runlog, silver  # noqa: E402

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
silver_schema = ns.schema("silver")
gold_schema = ns.schema("gold")
ops_schema = ns.schema("ops")
month = dbutils.widgets.get("month")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

assert re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month), f"month must be YYYY-MM, got {month!r}"
assert re.fullmatch(r"[A-Za-z0-9_.:-]+", batch_id), f"bad batch_id {batch_id!r}"
print(f"catalog={catalog} environment={environment} month={month} batch_id={batch_id}")

run = runlog.TaskRun(
    month,
    batch_id,
    "checks",
    runlog.spark_writer(spark, f"{ops_schema}.pipeline_runs"),  # noqa: F821 - spark is provided by Databricks
    runlog.spark_prior_attempts(spark, f"{ops_schema}.pipeline_runs"),  # noqa: F821
    job_run_id=dbutils.widgets.get("job_run_id"),  # noqa: F821
    task_run_id=dbutils.widgets.get("task_run_id"),  # noqa: F821
)

slice_filter = f"source_month = '{month}'"

# COMMAND ----------

with run.guard():
    # Bronze: duplicates are recomputed here with silver's own definition, not read back from silver.
    bronze = spark.table(f"{bronze_schema}.yellow_trips").where(  # noqa: F821
        F.col("_source_file").endswith(f"yellow_tripdata_{month}.parquet")
    )
    bronze_rows = bronze.count()
    run.note(rows_read=bronze_rows)
    typed = silver.cast_to_silver_schema(silver.standardise_columns(bronze))
    distinct_rows = typed.dropDuplicates(silver.SOURCE_COLUMNS + ["_source_file"]).count()

    silver_trips = spark.table(f"{silver_schema}.trips").where(slice_filter)  # noqa: F821
    quarantine = spark.table(f"{silver_schema}.trips_quarantine").where(slice_filter)  # noqa: F821
    fact = spark.table(f"{gold_schema}.fact_trips").where(slice_filter)  # noqa: F821

    silver_agg = silver_trips.agg(F.count("*").alias("n"), F.sum("total_amount").alias("total")).collect()[0]
    fact_agg_df = fact.agg(
        F.count("*").alias("n"),
        F.sum("total_amount").alias("total"),
        F.sum(F.xxhash64("trip_key").cast("decimal(38,0)")).alias("key_hash_sum"),
        F.sum(F.col("row_hash").cast("decimal(38,0)")).alias("row_hash_sum"),
    )
    fact_agg = fact_agg_df.collect()[0]

    orphans = sum(
        gold.count_orphans(fact, spark.table(f"{gold_schema}.{dim}"), fact_col, dim_col)  # noqa: F821
        for fact_col, dim, dim_col in [
            ("pu_zone_key", "dim_zone", "zone_key"),
            ("do_zone_key", "dim_zone", "zone_key"),
            ("pickup_date_key", "dim_date", "date_key"),
            ("pickup_hour_key", "dim_time_of_day", "hour_key"),
            ("payment_type_key", "dim_payment_type", "payment_type_key"),
        ]
    )
    agg_daily = (
        spark.table(f"{gold_schema}.agg_daily_revenue_by_borough")  # noqa: F821
        .where(slice_filter)
        .agg(F.sum("trips"))
        .collect()[0][0]
    )
    agg_hourly = (
        spark.table(f"{gold_schema}.agg_hourly_demand").where(slice_filter).agg(F.sum("trips")).collect()[0][0]  # noqa: F821
    )

    metrics = {
        "bronze_rows": bronze_rows,
        "distinct_rows": distinct_rows,
        "duplicates_removed": bronze_rows - distinct_rows,
        "silver_accepted": silver_agg["n"],
        "quarantined": quarantine.count(),
        "fact_rows": fact_agg["n"],
        "fact_total_amount": float(fact_agg["total"] or 0.0),
        "silver_total_amount": float(silver_agg["total"] or 0.0),
        "orphans": orphans,
        "agg_daily_trips": int(agg_daily or 0),
        "agg_hourly_trips": int(agg_hourly or 0),
    }
    failures = reconcile.evaluate(metrics)

# COMMAND ----------

with run.guard():
    status = "FAIL" if failures else "PASS"
    # The two hash sums stay inside Spark (decimal(38,0)); round-tripping them through Python would overflow the type.
    (
        fact_agg_df.select(
            F.lit(month).alias("source_month"),
            F.lit(batch_id).alias("run_batch_id"),
            F.current_timestamp().alias("checked_at"),
            F.lit(metrics["bronze_rows"]).cast("bigint").alias("bronze_rows"),
            F.lit(metrics["distinct_rows"]).cast("bigint").alias("distinct_rows"),
            F.lit(metrics["duplicates_removed"]).cast("bigint").alias("duplicates_removed"),
            F.lit(metrics["silver_accepted"]).cast("bigint").alias("silver_accepted"),
            F.lit(metrics["quarantined"]).cast("bigint").alias("quarantined"),
            F.lit(metrics["fact_rows"]).cast("bigint").alias("fact_rows"),
            F.lit(metrics["fact_total_amount"]).cast("double").alias("fact_total_amount"),
            F.lit(metrics["silver_total_amount"]).cast("double").alias("silver_total_amount"),
            F.lit(metrics["orphans"]).cast("bigint").alias("orphans"),
            F.lit(metrics["agg_daily_trips"]).cast("bigint").alias("agg_daily_trips"),
            F.lit(metrics["agg_hourly_trips"]).cast("bigint").alias("agg_hourly_trips"),
            F.col("key_hash_sum").alias("fact_key_hash_sum"),
            F.col("row_hash_sum").alias("fact_row_hash_sum"),
            F.lit(status).alias("status"),
            F.lit("; ".join(failures)).alias("failures"),
        )
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{ops_schema}.month_reconciliation")
    )

    print(
        f"CHECKS month={month} bronze={metrics['bronze_rows']} duplicates={metrics['duplicates_removed']} "
        f"accepted={metrics['silver_accepted']} quarantined={metrics['quarantined']} fact={metrics['fact_rows']} "
        f"orphans={metrics['orphans']} key_hash_sum={fact_agg['key_hash_sum']} row_hash_sum={fact_agg['row_hash_sum']} "
        f"status={status}"
    )
    run.note(rows_quarantined=int(metrics["quarantined"]))
    reconcile.assert_reconciled(metrics)
    print("CHECKS OK")

    run.succeed(
        rows_written=1,  # the one ops.month_reconciliation row
        details={
            "reconciliation_status": status,
            "silver_accepted": int(metrics["silver_accepted"]),
            "fact_rows": int(metrics["fact_rows"]),
            "duplicates_removed": int(metrics["duplicates_removed"]),
        },
    )
