"""Silver-layer transformations for TLC yellow-taxi trips.

Every function takes and returns a DataFrame (no I/O), so the same code runs under local PySpark in tests
and on Databricks. Invalid rows are never dropped: they are tagged with reason codes and split into a
quarantine frame. The only rows that leave the pipeline are exact duplicates, and those are counted.
"""

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

# Stable reason codes written to the quarantine table. Do not rename: downstream reports group by them.
NEGATIVE_FARE = "NEGATIVE_FARE"
ZERO_DISTANCE = "ZERO_DISTANCE"
NEGATIVE_DISTANCE = "NEGATIVE_DISTANCE"
PICKUP_OUTSIDE_MONTH = "PICKUP_OUTSIDE_MONTH"
DROPOFF_BEFORE_PICKUP = "DROPOFF_BEFORE_PICKUP"
NULL_TIMESTAMP = "NULL_TIMESTAMP"
REASON_CODES = (
    NEGATIVE_FARE,
    ZERO_DISTANCE,
    NEGATIVE_DISTANCE,
    PICKUP_OUTSIDE_MONTH,
    DROPOFF_BEFORE_PICKUP,
    NULL_TIMESTAMP,
)

# Target schema of the source columns (explicit; nothing is inferred).
SILVER_SOURCE_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType()),
        StructField("pickup_ts", TimestampNTZType()),
        StructField("dropoff_ts", TimestampNTZType()),
        StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("ratecode_id", LongType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("pu_location_id", IntegerType()),
        StructField("do_location_id", IntegerType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
    ]
)
SOURCE_COLUMNS = [f.name for f in SILVER_SOURCE_SCHEMA.fields]

# Lower-cased source name -> silver name. Matching ignores case, which absorbs `Airport_fee` vs `airport_fee`.
_COLUMN_MAP = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "ratecodeid": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}
# Columns that older or newer years may lack; they become typed nulls. Everything else is required.
OPTIONAL_COLUMNS = {"congestion_surcharge", "airport_fee", "cbd_congestion_fee"}

LINEAGE_COLUMNS = ["_source_file", "_batch_id"]
_SOURCE_FILE_MONTH = re.compile(r"yellow_tripdata_(\d{4}-\d{2})\.parquet")


def standardise_columns(df: DataFrame) -> DataFrame:
    """Rename source columns to silver names and add missing optional columns as nulls.

    Raises ValueError if a required column is absent. Lineage columns (`_source_file`, `_batch_id`) are kept.
    """
    renamed = {}
    for col in df.columns:
        target = _COLUMN_MAP.get(col.lower())
        if target is not None:
            renamed[col] = target
    missing = set(_COLUMN_MAP.values()) - set(renamed.values()) - OPTIONAL_COLUMNS
    if missing:
        raise ValueError(f"required columns missing from source: {sorted(missing)}")
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    for field in SILVER_SOURCE_SCHEMA.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df.select(*SOURCE_COLUMNS, *[c for c in LINEAGE_COLUMNS if c in df.columns])


def cast_to_silver_schema(df: DataFrame) -> DataFrame:
    """Cast every source column to its explicit silver type."""
    for field in SILVER_SOURCE_SCHEMA.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


def add_source_month(df: DataFrame) -> DataFrame:
    """Derive `source_month` ('YYYY-MM') from the `_source_file` name (null if the name does not match)."""
    month = F.regexp_extract(F.col("_source_file"), _SOURCE_FILE_MONTH.pattern, 1)
    return df.withColumn("source_month", F.when(month != "", month))


def drop_exact_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """Remove rows identical on every source column and source file; return (deduped, number removed)."""
    before = df.count()
    deduped = df.dropDuplicates(SOURCE_COLUMNS + ["_source_file"])
    return deduped, before - deduped.count()


def add_trip_duration(df: DataFrame) -> DataFrame:
    """`trip_duration_min` in minutes (null when either timestamp is null; negative if dropoff precedes pickup)."""
    seconds = F.col("dropoff_ts").cast("timestamp").cast("double") - F.col("pickup_ts").cast("timestamp").cast("double")
    return df.withColumn("trip_duration_min", seconds / 60.0)


def apply_quality_rules(df: DataFrame) -> DataFrame:
    """Add `reason_codes` (array<string>, empty when the row is valid). A row failing several rules keeps all codes."""
    month_start = F.to_date(F.concat(F.col("source_month"), F.lit("-01"))).cast("timestamp_ntz")
    month_end = F.add_months(F.to_date(F.concat(F.col("source_month"), F.lit("-01"))), 1).cast("timestamp_ntz")
    in_month = (F.col("pickup_ts") >= month_start) & (F.col("pickup_ts") < month_end)
    candidates = [
        F.when(F.col("fare_amount") < 0, F.lit(NEGATIVE_FARE)),
        F.when(F.col("trip_distance") == 0, F.lit(ZERO_DISTANCE)),
        F.when(F.col("trip_distance") < 0, F.lit(NEGATIVE_DISTANCE)),
        F.when(F.col("pickup_ts").isNotNull() & ~F.coalesce(in_month, F.lit(False)), F.lit(PICKUP_OUTSIDE_MONTH)),
        F.when(F.col("dropoff_ts") < F.col("pickup_ts"), F.lit(DROPOFF_BEFORE_PICKUP)),
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), F.lit(NULL_TIMESTAMP)),
    ]
    return df.withColumn("reason_codes", F.filter(F.array(*candidates), lambda code: code.isNotNull()))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a frame carrying `reason_codes` into (valid without the column, quarantine with it)."""
    failed = F.size(F.col("reason_codes")) > 0
    return df.filter(~failed).drop("reason_codes"), df.filter(failed)


def transform_bronze(bronze: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Full silver transform: returns (valid, quarantine, duplicates_removed).

    Order matters: standardise and cast, dedupe, then validate, so that
    bronze rows = valid + quarantine + duplicates_removed.
    """
    typed = cast_to_silver_schema(standardise_columns(bronze))
    deduped, removed = drop_exact_duplicates(add_source_month(typed))
    tagged = apply_quality_rules(add_trip_duration(deduped))
    valid, quarantine = split_valid_quarantine(tagged.withColumn("_processed_at", F.current_timestamp()))
    return valid, quarantine, removed
