import pytest

from taxilake.namespace import DEV_PREFIX, LAYERS, render_sql, resolve

TABLES = {
    "bronze": ["yellow_trips", "taxi_zones", "weather_hourly"],
    "silver": ["trips", "trips_quarantine"],
    "gold": ["dim_zone", "dim_date", "dim_time_of_day", "dim_payment_type", "fact_trips"],
    "ops": ["month_reconciliation", "failure_injection", "pipeline_runs", "smoke_test"],
}


def test_prod_keeps_the_original_names():
    ns = resolve("workspace", "prod")
    assert ns.table("bronze", "yellow_trips") == "workspace.bronze.yellow_trips"
    assert ns.table("silver", "trips_quarantine") == "workspace.silver.trips_quarantine"
    assert ns.table("gold", "fact_trips") == "workspace.gold.fact_trips"
    assert ns.table("ops", "pipeline_runs") == "workspace.ops.pipeline_runs"
    assert ns.volume_root == "/Volumes/workspace/landing/raw"


def test_dev_uses_prefixed_schemas_and_volume():
    ns = resolve("workspace", "dev")
    assert ns.table("bronze", "yellow_trips") == "workspace.dev_bronze.yellow_trips"
    assert ns.table("ops", "month_reconciliation") == "workspace.dev_ops.month_reconciliation"
    assert ns.volume_root == "/Volumes/workspace/dev_landing/raw"


@pytest.mark.parametrize("layer", LAYERS)
def test_dev_never_resolves_to_a_production_schema(layer):
    dev, prod = resolve("workspace", "dev"), resolve("workspace", "prod")
    assert dev.schema(layer) != prod.schema(layer)
    assert dev.schema(layer).split(".")[1].startswith(DEV_PREFIX)
    assert not prod.schema(layer).split(".")[1].startswith(DEV_PREFIX)


@pytest.mark.parametrize("layer,names", TABLES.items())
def test_no_dev_table_name_equals_a_prod_table_name(layer, names):
    dev, prod = resolve("workspace", "dev"), resolve("workspace", "prod")
    for name in names:
        assert dev.table(layer, name) != prod.table(layer, name)


def test_dev_and_prod_volumes_differ():
    assert resolve("workspace", "dev").volume_root != resolve("workspace", "prod").volume_root


@pytest.mark.parametrize("environment", ["", "DEV", "Prod", "staging", "dev ", None, "workspace"])
def test_unknown_environment_is_rejected(environment):
    with pytest.raises(ValueError):
        resolve("workspace", environment)


@pytest.mark.parametrize("catalog", ["", "a.b", "work space", "x;DROP", "a-b", None])
def test_bad_catalog_is_rejected(catalog):
    with pytest.raises(ValueError):
        resolve(catalog, "dev")


def test_unknown_layer_and_bad_table_name_are_rejected():
    ns = resolve("workspace", "dev")
    with pytest.raises(ValueError):
        ns.schema("platinum")
    for bad in ("", "a.b", "t; DROP TABLE x", None):
        with pytest.raises(ValueError):
            ns.table("gold", bad)


def test_render_sql_rewrites_qualified_names_and_volume_paths_for_dev():
    sql = (
        "CREATE TABLE IF NOT EXISTS workspace.silver.trips (a INT);\n"
        "ALTER TABLE workspace.silver.trips ADD CONSTRAINT c CHECK (a > 0);\n"
        "CREATE VOLUME IF NOT EXISTS workspace.landing.raw;\n"
        "-- files land at /Volumes/workspace/landing/raw/\n"
        "SHOW SCHEMAS IN workspace;\n"
    )
    out = render_sql(sql, "workspace", "dev")
    assert "workspace.dev_silver.trips" in out
    assert "workspace.dev_landing.raw" in out
    assert "/Volumes/workspace/dev_landing/raw/" in out
    assert "SHOW SCHEMAS IN workspace;" in out
    assert "workspace.silver" not in out and "workspace.landing" not in out


def test_render_sql_for_prod_is_the_identity():
    sql = "CREATE TABLE IF NOT EXISTS workspace.gold.fact_trips (a INT);\n"
    assert render_sql(sql, "workspace", "prod") == sql


def test_render_sql_does_not_touch_look_alike_names():
    sql = "SELECT 1 FROM myworkspace.gold.t, other.workspace.gold.t"
    assert render_sql(sql, "workspace", "dev") == sql
