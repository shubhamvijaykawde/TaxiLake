# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze load
# MAGIC Loads one month of landed files from the volume into the bronze tables with `COPY INTO`.
# MAGIC `COPY INTO` remembers which files it already loaded per target table, so re-running the same month adds zero rows.
# MAGIC Requires `sql/10_bronze_tables.sql` and the files uploaded by `scripts/upload_to_volume.py`.

# COMMAND ----------

import re
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
dbutils.widgets.text("month", "2025-01")  # noqa: F821
dbutils.widgets.text("batch_id", "")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
month = dbutils.widgets.get("month")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Values are interpolated into SQL text below, so only accept simple, known shapes.
assert re.fullmatch(r"[A-Za-z0-9_]+", catalog), f"bad catalog {catalog!r}"
assert re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month), f"month must be YYYY-MM, got {month!r}"
assert re.fullmatch(r"[A-Za-z0-9_.:-]+", batch_id), f"bad batch_id {batch_id!r}"

root = f"/Volumes/{catalog}/landing/raw"
print(f"catalog={catalog} month={month} batch_id={batch_id}")

META = f"_metadata.file_path AS _source_file, current_timestamp() AS _ingested_at, '{batch_id}' AS _batch_id"

# COMMAND ----------


def copy_into(table: str, source_dir: str, fmt: str, pattern: str, options: str = "", columns: str = "*") -> None:
    before = spark.table(f"{catalog}.bronze.{table}").count()  # noqa: F821 - spark is provided by Databricks
    result = spark.sql(  # noqa: F821
        f"""
        COPY INTO {catalog}.bronze.{table}
        FROM (SELECT {columns}, {META} FROM '{source_dir}')
        FILEFORMAT = {fmt}
        PATTERN = '{pattern}'
        {options}
        """
    ).collect()[0]
    after = spark.table(f"{catalog}.bronze.{table}").count()  # noqa: F821
    print(f"{table}: before={before} inserted_by_copy_into={result['num_inserted_rows']} after={after}")


copy_into(
    "yellow_trips",
    f"{root}/yellow/{month}",
    "PARQUET",
    f"yellow_tripdata_{month}.parquet",
)
copy_into(
    "taxi_zones",
    f"{root}/zones",
    "CSV",
    "taxi_zone_lookup.csv",
    "FORMAT_OPTIONS ('header' = 'true')",
    # CSV columns arrive as strings; the only cast is LocationID to match the explicit table schema.
    columns="CAST(LocationID AS INT) AS LocationID, Borough, Zone, service_zone",
)
copy_into(
    "weather_hourly",
    f"{root}/weather/{month}",
    "JSON",
    f"weather_{month}.jsonl",
)
