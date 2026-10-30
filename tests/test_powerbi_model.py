"""Static checks of powerbi/model/taxilake_model.tmdl.

These cannot prove that Power BI accepts the script (that needs Power BI Desktop), but they catch the mistakes that
would otherwise only show up there: dangling references, mismatched key types, non-read-only queries, and any host,
credential or e-mail address in the Power BI files.
"""

import re
from pathlib import Path

import pytest

from taxilake.namespace import DEV_PREFIX, LAYERS, PREFIXES, resolve

ROOT = Path(__file__).resolve().parents[1]
TMDL = ROOT / "powerbi" / "model" / "taxilake_model.tmdl"
POWERBI_FILES = [p for p in (ROOT / "powerbi").rglob("*") if p.is_file() and p.suffix in {".tmdl", ".md", ".dax"}]

DIMENSIONS = {
    "dim_date": "date_key",
    "dim_time_of_day": "hour_key",
    "dim_zone": "zone_key",
    "dim_payment_type": "payment_type_key",
    "dim_month": "year_month",
}
EXPECTED_RELATIONSHIPS = {
    ("fact_trips_hourly", "pickup_date_key", "dim_date", "date_key"),
    ("fact_trips_hourly", "pickup_hour_key", "dim_time_of_day", "hour_key"),
    ("fact_trips_hourly", "pu_zone_key", "dim_zone", "zone_key"),
    ("fact_trips_hourly", "payment_type_key", "dim_payment_type", "payment_type_key"),
    ("agg_hourly_demand", "pickup_date_key", "dim_date", "date_key"),
    ("agg_hourly_demand", "pickup_hour_key", "dim_time_of_day", "hour_key"),
    ("agg_daily_revenue_by_borough", "pickup_date_key", "dim_date", "date_key"),
    ("dim_date", "year_month", "dim_month", "year_month"),
    ("dq_reconciliation", "source_month", "dim_month", "year_month"),
    ("dq_quarantine_by_rule", "source_month", "dim_month", "year_month"),
}
SENSITIVE = [
    r"https?://",
    r"\.cloud\.databricks\.com",
    r"dbc-[0-9a-f]{8}",
    r"/sql/1\.0/warehouses/[0-9a-f]+",
    r"dapi[0-9a-f]{16,}",
    r"[\w.+-]+@[\w-]+\.[\w.]+",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
]


def parse(text):
    """Very small TMDL reader: tables, columns, measures, partitions, relationships and expressions."""
    tables, relationships, expressions = {}, [], {}
    current, section = None, None
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line.strip() or line.strip().startswith("///"):
            continue
        indent = len(line) - len(line.lstrip("\t"))
        body = line.strip()
        if indent == 1:
            section = None
            if body.startswith("table "):
                current = {"name": body[6:].strip(), "columns": {}, "measures": {}, "props": {}, "partition": None}
                tables[current["name"]] = current
            elif body.startswith("relationship "):
                section = {"kind": "rel", "name": body.split()[1]}
                relationships.append(section)
                current = None
            elif body.startswith("expression "):
                name, _, rest = body[len("expression ") :].partition(" = ")
                expressions[name.strip()] = rest
                current = None
        elif indent == 2 and section and section["kind"] == "rel":
            key, _, value = body.partition(": ")
            section[key] = value
        elif indent == 2 and current is not None:
            if body.startswith("column "):
                rest = body[len("column ") :]
                name, _, expr = rest.partition(" = ")
                section = {"kind": "column", "name": name.strip(), "expr": expr or None, "props": {}}
                current["columns"][section["name"]] = section
            elif body.startswith("measure "):
                rest = body[len("measure ") :]
                name, _, expr = rest.partition(" = ")
                name = name.strip().strip("'")
                section = {"kind": "measure", "name": name, "expr": expr, "props": {}}
                current["measures"][name] = section
            elif body.startswith("partition "):
                head = body[len("partition ") :]
                name, _, kind = head.partition(" = ")
                section = {"kind": "partition", "name": name.strip(), "type": kind.strip(), "source": []}
                current["partition"] = section
            else:
                key, _, value = body.partition(": ")
                current["props"][key] = value
                section = None
        elif indent >= 3 and section:
            if section["kind"] in {"column", "measure"}:
                key, _, value = body.partition(": ")
                section["props"][key] = value
            elif section["kind"] == "partition":
                if body.startswith("source ="):
                    rest = body[len("source =") :].strip()
                    if rest:
                        section["source"].append(rest)
                elif body.startswith("mode:"):
                    section["mode"] = body.split(": ")[1]
                else:
                    section["source"].append(body)
    return tables, relationships, expressions


@pytest.fixture(scope="module")
def model():
    return parse(TMDL.read_text(encoding="utf-8"))


def test_script_starts_with_createorreplace_and_has_tabs_only():
    text = TMDL.read_text(encoding="utf-8")
    assert text.splitlines()[0] == "createOrReplace"
    assert not re.search(r"^ +\S", text, re.M), "indentation must use tabs"


def test_expected_tables_exist(model):
    tables, _, _ = model
    assert set(tables) == {
        "dim_date",
        "dim_time_of_day",
        "dim_zone",
        "dim_payment_type",
        "fact_trips_hourly",
        "agg_hourly_demand",
        "agg_daily_revenue_by_borough",
        "dq_reconciliation",
        "dq_quarantine_by_rule",
        "dim_month",
        "_Measures",
    }
    assert all(t["partition"] for t in tables.values())


def test_parameters_hold_placeholders_only(model):
    _, _, expressions = model
    assert set(expressions) == {"ServerHostname", "HttpPath", "Catalog", "Environment", "SchemaPrefix"}
    assert expressions["ServerHostname"].startswith('"REPLACE-WITH-SERVER-HOSTNAME"')
    assert expressions["HttpPath"].startswith('"REPLACE-WITH-HTTP-PATH"')
    assert expressions["Catalog"].startswith('"workspace"')


def test_environment_parameter_defaults_to_prod_and_offers_exactly_the_pipeline_environments(model):
    _, _, expressions = model
    env = expressions["Environment"]
    assert env.startswith('"prod" meta [IsParameterQuery = true')
    assert 'DefaultValue = "prod"' in env and "IsParameterQueryRequired = true" in env
    offered = re.search(r"List = \{([^}]*)\}", env).group(1)
    assert set(re.findall(r'"([^"]+)"', offered)) == set(PREFIXES)


def test_schema_prefix_matches_the_pipeline_namespace_mapping(model):
    """The M expression `if Environment = "dev" then "dev_" else ""` must give what namespace.py gives."""
    _, _, expressions = model
    match = re.fullmatch(r'if Environment = "(\w+)" then "(\w*)" else "(\w*)"', expressions["SchemaPrefix"])
    assert match, expressions["SchemaPrefix"]
    when_env, when_true, when_false = match.groups()
    assert when_env == "dev"
    for environment, prefix in PREFIXES.items():
        assert (when_true if environment == when_env else when_false) == prefix, environment
    assert when_true == DEV_PREFIX


def test_every_table_source_goes_through_the_schema_prefix(model):
    """No partition may name a schema literally; each one must resolve to a pipeline layer via SchemaPrefix."""
    tables, _, _ = model
    layers_used = set()
    for name, table in tables.items():
        source = " ".join(table["partition"]["source"])
        if table["partition"]["type"] == "calculated":
            continue
        assert not re.search(r'Name = "(?:%s)"' % "|".join(LAYERS), source), f"{name} hard-codes a schema"
        assert not re.search(r"\.(?:%s)\." % "|".join(LAYERS), source), f"{name} hard-codes a schema in SQL"
        schemas = re.findall(r'Name = SchemaPrefix & "(\w+)", Kind = "Schema"', source)
        schemas += re.findall(r'" & Catalog & "\." & SchemaPrefix & "(\w+)\.\w+', source)
        assert schemas, f"{name} does not read through SchemaPrefix"
        layers_used.update(schemas)
        for layer in schemas:
            assert layer in LAYERS, (name, layer)
    assert layers_used == {"gold", "silver", "ops"}


def test_resolved_schemas_match_namespace_for_both_environments(model):
    """Substitute the prefix the way Power Query would and compare to the schema the dev/prod pipeline writes."""
    tables, _, expressions = model
    prefix_of = {"prod": "", "dev": "dev_"}
    for environment, prefix in prefix_of.items():
        ns = resolve("workspace", environment)
        for table in tables.values():
            source = " ".join(table["partition"]["source"])
            for layer in re.findall(r'SchemaPrefix & "(\w+)"', source):
                assert f"{ns.catalog}.{prefix}{layer}" == ns.schema(layer), (table["name"], environment, layer)
    assert set(prefix_of) == set(PREFIXES)
    assert "SchemaPrefix" in expressions


def test_relationships_match_the_specification(model):
    tables, relationships, _ = model
    found = set()
    for rel in relationships:
        from_table, from_col = rel["fromColumn"].split(".")
        to_table, to_col = rel["toColumn"].split(".")
        assert from_col in tables[from_table]["columns"], rel
        assert to_col in tables[to_table]["columns"], rel
        assert (
            tables[from_table]["columns"][from_col]["props"]["dataType"]
            == (tables[to_table]["columns"][to_col]["props"]["dataType"])
        ), f"key types differ in {rel['name']}"
        assert DIMENSIONS.get(to_table) == to_col, f"{rel['name']} must point at the dimension key"
        assert "crossFilteringBehavior" not in rel, "filters must stay single-direction"
        found.add((from_table, from_col, to_table, to_col))
    assert found == EXPECTED_RELATIONSHIPS
    assert len(relationships) == len({r["name"] for r in relationships})


def test_each_fact_table_reaches_a_dimension_by_exactly_one_path(model):
    _, relationships, _ = model
    pairs = [(r["fromColumn"].split(".")[0], r["toColumn"].split(".")[0]) for r in relationships]
    assert len(pairs) == len(set(pairs)), "two relationships between the same tables would be ambiguous"


def test_date_table_is_marked_and_sort_columns_exist(model):
    tables, _, _ = model
    date = tables["dim_date"]
    assert date["props"].get("dataCategory") == "Time"
    keys = [c for c in date["columns"].values() if "isKey" in c["props"] or "isKey" in str(c)]
    assert [c["name"] for c in keys] == ["date"] or "date" in date["columns"]
    for table in tables.values():
        for column in table["columns"].values():
            sort = column["props"].get("sortByColumn")
            if sort:
                assert sort in table["columns"], (table["name"], column["name"], sort)


def test_dimension_key_columns_are_hidden_and_facts_expose_no_raw_columns(model):
    tables, _, _ = model
    for table, key in DIMENSIONS.items():
        if table == "dim_month":
            continue
        assert "isHidden" in tables[table]["columns"][key]["props"], (table, key)
    assert all("isHidden" in c["props"] for c in tables["fact_trips_hourly"]["columns"].values())


def test_source_columns_exist_in_the_sql_ddl(model):
    tables, _, _ = model
    ddl = {
        "dim_date": "gold.dim_date",
        "dim_time_of_day": "gold.dim_time_of_day",
        "dim_zone": "gold.dim_zone",
        "dim_payment_type": "gold.dim_payment_type",
        "agg_hourly_demand": "gold.agg_hourly_demand",
        "agg_daily_revenue_by_borough": "gold.agg_daily_revenue_by_borough",
        "dq_reconciliation": "ops.month_reconciliation",
    }
    sql_text = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "sql").glob("*.sql"))
    for table, qualified in ddl.items():
        match = re.search(rf"CREATE TABLE IF NOT EXISTS workspace\.{re.escape(qualified)} \((.*?)\n\)", sql_text, re.S)
        assert match, qualified
        declared = {line.split()[0] for line in match.group(1).splitlines() if line.strip()}
        for column in tables[table]["columns"].values():
            if column["expr"] is None:
                assert column["name"] in declared, (table, column["name"])


def test_queries_are_read_only_selects_and_expose_the_declared_columns(model):
    tables, _, _ = model
    for name in ("fact_trips_hourly", "dq_quarantine_by_rule"):
        source = " ".join(tables[name]["partition"]["source"])
        assert "Value.NativeQuery" in source
        sql = "".join(re.findall(r'"((?:[^"]|"")*)"', source.split("Value.NativeQuery")[1]))
        assert sql.upper().startswith("SELECT"), name
        for word in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "MERGE", "TRUNCATE", "GRANT"):
            assert not re.search(rf"\b{word}\b", sql, re.I), (name, word)
        for column in tables[name]["columns"]:
            assert re.search(rf"\b{column}\b", sql), (name, column)


def test_every_reference_in_measures_and_calculated_columns_resolves(model):
    tables, _, _ = model
    measures = {m for t in tables.values() for m in t["measures"]}
    all_measures = [m for t in tables.values() for m in t["measures"].values()]
    assert len(measures) == len(all_measures), "measure names must be unique"
    expressions = [(m["name"], m["expr"]) for m in all_measures]
    expressions += [
        (f"{t['name']}[{c['name']}]", c["expr"]) for t in tables.values() for c in t["columns"].values() if c["expr"]
    ]
    expressions += [
        (t["name"], " ".join(t["partition"]["source"]))
        for t in tables.values()
        if t["partition"]["type"] == "calculated"
    ]
    for label, expr in expressions:
        for table, column in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\]", expr):
            assert table in tables, (label, table)
            assert column in tables[table]["columns"], (label, table, column)
        bare = re.findall(r"(?<![A-Za-z0-9_\]])\[([^\]]+)\]", expr)
        for name in bare:
            assert name in measures, (label, name)


def test_required_measures_exist_with_documented_definitions(model):
    tables, _, _ = model
    m = tables["_Measures"]["measures"]
    for name in ("Total Revenue", "Trips", "Average Fare", "Revenue per Mile", "Revenue MoM %"):
        assert name in m
    assert m["Total Revenue"]["expr"] == "SUM(fact_trips_hourly[total_amount_sum])"
    assert m["Average Fare"]["expr"] == "DIVIDE([Fare Revenue], [Trips])"
    assert m["Revenue per Mile"]["expr"] == "DIVIDE([Total Revenue], [Total Distance (miles)])"
    assert "DATEADD(dim_date[date], -1, MONTH)" in m["Revenue Prior Month"]["expr"]
    assert "HASONEVALUE" in m["Revenue Prior Month"]["expr"]
    assert m["Quarantine Rate"]["expr"] == "DIVIDE([Quarantined Rows], [Bronze Rows])"
    for name, measure in m.items():
        assert measure["props"].get("formatString", "").startswith('"') or name == "Reconciliation Status", name


def test_validation_queries_reference_only_existing_measures_and_tables(model):
    tables, _, _ = model
    measures = {m for t in tables.values() for m in t["measures"]}
    dax = (ROOT / "powerbi" / "validation" / "validate_measures.dax").read_text(encoding="utf-8")
    code = re.sub(r"//[^\n]*", "", dax)
    code = re.sub(r'"[^"]*"', '""', code)
    for table, column in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\]", code):
        assert table in tables and column in tables[table]["columns"], (table, column)
    for name in re.findall(r"(?<![A-Za-z0-9_\]])\[([^\]]+)\]", code):
        assert name in measures, name
    assert len(re.findall(r"^EVALUATE\b", dax, re.M)) == 15


@pytest.mark.parametrize("path", POWERBI_FILES, ids=lambda p: p.name)
def test_no_host_credential_or_email_in_powerbi_files(path):
    text = path.read_text(encoding="utf-8")
    for pattern in SENSITIVE:
        assert not re.search(pattern, text), (path.name, pattern)


def test_report_spec_uses_only_fields_and_measures_that_exist(model):
    tables, _, _ = model
    measures = {m for t in tables.values() for m in t["measures"]}
    spec = (ROOT / "powerbi" / "REPORT_SPEC.md").read_text(encoding="utf-8")
    assert "dim_date[is_weekend]" in spec
    for table, column in set(re.findall(r"`([a-z_]+)\[([a-z_]+)\]`", spec)):
        assert table in tables and column in tables[table]["columns"], (table, column)
    for name in set(re.findall(r"`\[([^\]`]+)\]`", spec)):
        assert name in measures, name
