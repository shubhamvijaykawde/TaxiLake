"""The dev DDL is generated from the production DDL, so it must not leave any production name behind."""

import re
from pathlib import Path

import pytest
from render_sql import SQL_FILES, render_files

ROOT = Path(__file__).resolve().parents[1]
LAYERS = "landing|bronze|silver|gold|ops"
PROD_NAME = re.compile(rf"workspace\.({LAYERS})\b|/Volumes/workspace/({LAYERS})/")


def test_every_listed_sql_file_exists_and_unlisted_ones_are_known():
    on_disk = {p.name for p in (ROOT / "sql").glob("*.sql")}
    assert set(SQL_FILES) <= on_disk
    assert on_disk - set(SQL_FILES) == {"31_migrate_fact_row_hash.sql"}


@pytest.mark.parametrize("name", SQL_FILES)
def test_rendered_dev_ddl_contains_no_production_names(name):
    original = (ROOT / "sql" / name).read_text(encoding="utf-8")
    rendered = render_files("dev", only=name)
    assert PROD_NAME.search(original), "the source DDL should be production-qualified"
    assert not PROD_NAME.search(rendered), PROD_NAME.findall(rendered)
    assert "workspace.dev_" in rendered


def test_every_create_statement_in_the_dev_ddl_targets_a_dev_schema():
    rendered = render_files("dev")
    for statement in re.findall(r"(?:CREATE|ALTER)\s+(?:SCHEMA|TABLE|VOLUME)\s+(?:IF NOT EXISTS\s+)?(\S+)", rendered):
        assert statement.startswith("workspace.dev_"), statement


def test_prod_rendering_is_the_unchanged_production_ddl():
    for name in SQL_FILES:
        original = (ROOT / "sql" / name).read_text(encoding="utf-8")
        assert original in render_files("prod", only=name)
