"""Gold-layer star schema builders for TLC yellow-taxi trips.

DataFrame in, DataFrame out (no I/O), so the same code runs under local PySpark in tests and on Databricks.
Grain of `fact_trips`: one row per accepted trip (silver row), identified by `trip_key`.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxilake.silver import SOURCE_COLUMNS

TRIP_KEY_COLUMNS = SOURCE_COLUMNS + ["_source_file"]

# TLC data dictionary payment types (static dimension; unknown future codes would show up as orphans, not vanish).
PAYMENT_TYPES = (
    (0, "Flex Fare trip"),
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip"),
)

FACT_COLUMNS = [
    "trip_key",
    "pickup_ts",
    "dropoff_ts",
    "pickup_date_key",
    "pickup_hour_key",
    "pu_zone_key",
    "do_zone_key",
    "payment_type_key",
    "vendor_id",
    "ratecode_id",
    "passenger_count",
    "trip_distance",
    "trip_duration_min",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "total_amount",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "weather_code",
    "source_month",
    "_batch_id",
    "row_hash",
]
# Columns whose values define a fact row's content; _batch_id is excluded because it changes on every run.
HASHED_COLUMNS = [c for c in FACT_COLUMNS if c not in ("_batch_id", "row_hash")]


def add_trip_key(df: DataFrame) -> DataFrame:
    """SHA-256 of all source columns plus the source file.

    JSON of a struct (with nulls kept) is used instead of concat_ws, which silently skips nulls and could make
    two different rows hash alike. Unique because silver already removed exact duplicates.
    """
    payload = F.to_json(F.struct(*[F.col(c) for c in TRIP_KEY_COLUMNS]), {"ignoreNullFields": "false"})
    return df.withColumn("trip_key", F.sha2(payload, 256))


def build_dim_zone(zones: DataFrame) -> DataFrame:
    """Zone dimension keyed by the TLC LocationID. The Unknown / N/A zones stay as real members."""
    return zones.select(
        F.col("LocationID").cast("int").alias("zone_key"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    ).dropDuplicates(["zone_key"])


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per calendar day from start to end inclusive."""
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{start.isoformat()}', DATE'{end.isoformat()}', INTERVAL 1 DAY)) AS date"
    )
    iso_dow = ((F.dayofweek("date") + 5) % 7) + 1  # 1 = Monday ... 7 = Sunday
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        iso_dow.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (iso_dow >= 6).alias("is_weekend"),
    )


def time_band(hour: int) -> str:
    """Pure-Python reference for the band of an hour (also used to document the Spark expression)."""
    if hour < 0 or hour > 23:
        raise ValueError(f"hour must be 0-23, got {hour}")
    if hour < 6:
        return "night"
    if hour < 12:
        return "morning"
    if hour < 18:
        return "afternoon"
    return "evening"


def build_dim_time_of_day(spark: SparkSession) -> DataFrame:
    """24 rows, one per hour of day."""
    hours = spark.range(0, 24).select(F.col("id").cast("int").alias("hour_key"))
    band = (
        F.when(F.col("hour_key") < 6, "night")
        .when(F.col("hour_key") < 12, "morning")
        .when(F.col("hour_key") < 18, "afternoon")
        .otherwise("evening")
    )
    return hours.select(
        "hour_key",
        F.format_string("%02d:00", F.col("hour_key")).alias("hour_label"),
        band.alias("time_band"),
    )


def build_dim_payment_type(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(PAYMENT_TYPES, "payment_type_key int, payment_type_name string")


def build_fact_trips(silver_trips: DataFrame, weather: DataFrame) -> DataFrame:
    """Fact rows from accepted silver trips, with hourly weather attached by a LEFT join on the pickup hour.

    A left join keeps every trip even when weather is missing (the weather columns are then null). The weather
    `time` string is cast to a timestamp here because bronze kept it raw. Weather must hold one row per hour,
    otherwise the join would fan out; callers reconcile fact rows against silver rows to catch that.
    """
    wx = weather.select(
        F.to_timestamp_ntz("time").alias("_wx_hour"),
        F.col("temperature_2m").alias("temperature_c"),
        F.col("precipitation").alias("precipitation_mm"),
        F.col("wind_speed_10m").alias("wind_speed_kmh"),
        F.col("weather_code").cast("int").alias("weather_code"),
    )
    keyed = add_trip_key(silver_trips).withColumn(
        "_pickup_hour", F.date_trunc("hour", F.col("pickup_ts")).cast("timestamp_ntz")
    )
    joined = keyed.join(wx, keyed["_pickup_hour"] == wx["_wx_hour"], "left")
    built = joined.select(
        "trip_key",
        "pickup_ts",
        "dropoff_ts",
        F.date_format("pickup_ts", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.hour("pickup_ts").alias("pickup_hour_key"),
        F.col("pu_location_id").alias("pu_zone_key"),
        F.col("do_location_id").alias("do_zone_key"),
        F.col("payment_type").cast("int").alias("payment_type_key"),
        "vendor_id",
        "ratecode_id",
        "passenger_count",
        "trip_distance",
        "trip_duration_min",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "total_amount",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_kmh",
        "weather_code",
        "source_month",
        "_batch_id",
    )
    # Content hash used by the MERGE to skip rows that did not change.
    return built.withColumn("row_hash", F.xxhash64(*[F.col(c) for c in HASHED_COLUMNS]))


def count_orphans(fact: DataFrame, dim: DataFrame, fact_col: str, dim_col: str) -> int:
    """Number of fact rows with a non-null key that has no matching dimension row."""
    return (
        fact.where(F.col(fact_col).isNotNull())
        .join(dim.select(F.col(dim_col).alias("_dim_key")), F.col(fact_col) == F.col("_dim_key"), "left_anti")
        .count()
    )


def assert_no_orphans(fact: DataFrame, dim: DataFrame, fact_col: str, dim_col: str, name: str) -> None:
    """Raise ValueError if any fact row has a non-null key with no matching dimension row."""
    orphans = count_orphans(fact, dim, fact_col, dim_col)
    if orphans:
        raise ValueError(f"{orphans} fact rows have no {name} row ({fact_col})")


def daily_revenue_by_borough(fact: DataFrame, dim_zone: DataFrame) -> DataFrame:
    """Per pickup day and pickup borough: trips and revenue measures."""
    zone = dim_zone.select(F.col("zone_key").alias("_zk"), F.col("borough").alias("pickup_borough"))
    joined = fact.join(zone, fact["pu_zone_key"] == zone["_zk"], "left")
    return joined.groupBy("source_month", "pickup_date_key", "pickup_borough").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("fare_amount").alias("fare_revenue"),
        F.sum("tip_amount").alias("tip_revenue"),
        F.sum("trip_distance").alias("total_distance"),
    )


def hourly_demand(fact: DataFrame) -> DataFrame:
    """Per pickup day and hour: trips, revenue, and the (single) weather reading for that hour."""
    return fact.groupBy("source_month", "pickup_date_key", "pickup_hour_key").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.first("temperature_c", ignorenulls=True).alias("temperature_c"),
        F.first("precipitation_mm", ignorenulls=True).alias("precipitation_mm"),
    )
