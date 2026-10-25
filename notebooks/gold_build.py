# Databricks notebook source
# MAGIC %md
# MAGIC # Gold build
# MAGIC Rebuilds the dimensions, MERGEs one month of `silver.trips` into `gold.fact_trips` on `trip_key`, rebuilds that month's
# MAGIC aggregates, and asserts: unique keys, no orphans, fact rows = silver rows, totals reconcile with silver.
# MAGIC Requires `sql/30_gold_tables.sql`.

# COMMAND ----------

import os
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(notebook_path)) + "/src")

from taxilake import gold  # noqa: E402

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
dbutils.widgets.text("month", "2025-01")  # noqa: F821
dbutils.widgets.text("batch_id", "")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
month = dbutils.widgets.get("month")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

assert re.fullmatch(r"[A-Za-z0-9_]+", catalog), f"bad catalog {catalog!r}"
assert re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month), f"month must be YYYY-MM, got {month!r}"
assert re.fullmatch(r"[A-Za-z0-9_.:-]+", batch_id), f"bad batch_id {batch_id!r}"
print(f"catalog={catalog} month={month} batch_id={batch_id}")

slice_filter = f"source_month = '{month}'"
silver_month = spark.table(f"{catalog}.silver.trips").where(slice_filter)  # noqa: F821 - spark is provided by Databricks
silver_rows = silver_month.count()
assert silver_rows > 0, f"no silver rows for {month}; run the silver transform first"

# COMMAND ----------

# Dimensions: small, fully rebuilt. dim_date spans all silver data so earlier months are never dropped.
bounds = (
    spark.table(f"{catalog}.silver.trips")
    .agg(  # noqa: F821
        F.min(F.to_date("pickup_ts")).alias("lo"), F.max(F.to_date("pickup_ts")).alias("hi")
    )
    .collect()[0]
)
dims = {
    "dim_zone": gold.build_dim_zone(spark.table(f"{catalog}.bronze.taxi_zones")),  # noqa: F821
    "dim_date": gold.build_dim_date(spark, bounds["lo"], bounds["hi"]),  # noqa: F821
    "dim_time_of_day": gold.build_dim_time_of_day(spark),  # noqa: F821
    "dim_payment_type": gold.build_dim_payment_type(spark),  # noqa: F821
}
for name, df in dims.items():
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.{name}")
    print(f"{name}: {spark.table(f'{catalog}.gold.{name}').count()} rows")  # noqa: F821

# COMMAND ----------

# Fact: MERGE on trip_key. Matched rows are updated only when their content hash changed, new rows are inserted, and rows
# of this month that silver no longer has are deleted, so the month's slice always equals silver and an unchanged re-run
# writes nothing.
weather = spark.table(f"{catalog}.bronze.weather_hourly")  # noqa: F821
fact_new = gold.build_fact_trips(silver_month, weather).withColumn("_batch_id", F.lit(batch_id))
fact_new.createOrReplaceTempView("fact_new")

merge = spark.sql(  # noqa: F821
    f"""
    MERGE INTO {catalog}.gold.fact_trips AS t
    USING fact_new AS s
    ON t.trip_key = s.trip_key
    WHEN MATCHED AND NOT (t.row_hash <=> s.row_hash) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND t.{slice_filter} THEN DELETE
    """
).collect()[0]
print(
    f"MERGE: affected={merge['num_affected_rows']} updated={merge['num_updated_rows']} "
    f"inserted={merge['num_inserted_rows']} deleted={merge['num_deleted_rows']}"
)

# COMMAND ----------

fact = spark.table(f"{catalog}.gold.fact_trips").where(slice_filter)  # noqa: F821
dim_zone = spark.table(f"{catalog}.gold.dim_zone")  # noqa: F821


def replace_slice(df, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("replaceWhere", slice_filter).saveAsTable(
        f"{catalog}.gold.{table}"
    )


replace_slice(gold.daily_revenue_by_borough(fact, dim_zone), "agg_daily_revenue_by_borough")
replace_slice(gold.hourly_demand(fact), "agg_hourly_demand")

# COMMAND ----------

# Checks, computed from the written tables.
fact_rows = fact.count()
distinct_keys = fact.select("trip_key").distinct().count()
print(f"silver_rows={silver_rows} fact_rows={fact_rows} distinct_trip_keys={distinct_keys}")
assert distinct_keys == fact_rows, "trip_key is not unique"
assert fact_rows == silver_rows, "fact rows do not equal silver rows"

for fact_col, dim_name, dim_col in [
    ("pu_zone_key", "dim_zone", "zone_key"),
    ("do_zone_key", "dim_zone", "zone_key"),
    ("pickup_date_key", "dim_date", "date_key"),
    ("pickup_hour_key", "dim_time_of_day", "hour_key"),
    ("payment_type_key", "dim_payment_type", "payment_type_key"),
]:
    gold.assert_no_orphans(fact, spark.table(f"{catalog}.gold.{dim_name}"), fact_col, dim_col, dim_name)  # noqa: F821
print("orphans=0 for all 5 dimension keys")

sums = ["total_amount", "fare_amount", "trip_distance"]
silver_tot = silver_month.agg(*[F.sum(c).alias(c) for c in sums]).collect()[0]
fact_tot = fact.agg(*[F.sum(c).alias(c) for c in sums]).collect()[0]
for c in sums:
    print(f"{c}: silver={silver_tot[c]:.4f} fact={fact_tot[c]:.4f} diff={abs(silver_tot[c] - fact_tot[c]):.6f}")
    assert abs(silver_tot[c] - fact_tot[c]) < 0.01, f"{c} does not reconcile"

for table in ("agg_daily_revenue_by_borough", "agg_hourly_demand"):
    agg = spark.table(f"{catalog}.gold.{table}").where(slice_filter)  # noqa: F821
    got = agg.agg(F.sum("trips").alias("trips"), F.sum("total_revenue").alias("rev")).collect()[0]
    print(f"{table}: rows={agg.count()} trips={got['trips']} revenue={got['rev']:.4f}")
    assert got["trips"] == fact_rows, f"{table} trips do not match fact"
    assert abs(got["rev"] - fact_tot["total_amount"]) < 0.01, f"{table} revenue does not match fact"
print("GOLD CHECKS OK")
