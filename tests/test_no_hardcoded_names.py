"""Static guard: environment-specific names are built only by taxilake.namespace, never typed into code."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CODE = sorted(
    [*(ROOT / "notebooks").glob("*.py"), *(ROOT / "scripts").glob("*.py"), *(ROOT / "src" / "taxilake").glob("*.py")]
)
LAYERS = "landing|bronze|silver|gold|ops"
# A literal catalog-qualified schema, a literal volume path, or the old {catalog}.<layer>. interpolation.
HARD_CODED = [
    re.compile(rf"workspace\.({LAYERS})\b"),
    re.compile(r"/Volumes/workspace\b"),
    re.compile(rf"\{{catalog\}}\.({LAYERS})\b"),
    re.compile(rf"\{{catalog\}}/({LAYERS})\b"),
]
# namespace.py is the one place allowed to mention these shapes (in docstrings and the SQL renderer).
ALLOWED = {"namespace.py"}


@pytest.mark.parametrize("path", [p for p in CODE if p.name not in ALLOWED], ids=lambda p: p.name)
def test_no_hard_coded_catalog_or_schema_in_code(path):
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for pattern in HARD_CODED:
            assert not pattern.search(line), f"{path.name}:{number} hard-codes a production name: {line.strip()}"


PIPELINE_NOTEBOOKS = ["bronze_load", "silver_transform", "gold_build", "checks", "freshness_check", "phase0_smoke_test"]


@pytest.mark.parametrize("name", PIPELINE_NOTEBOOKS)
def test_notebooks_resolve_their_names_through_the_namespace_module(name):
    text = (ROOT / "notebooks" / f"{name}.py").read_text(encoding="utf-8")
    assert 'dbutils.widgets.text("environment", "dev")' in text, "the widget default must be the safe one (dev)"
    assert 'environment = dbutils.widgets.get("environment")' in text
    assert "namespace.resolve(catalog, environment)" in text
    assert re.search(r"\bops_schema = ns\.schema\(\"ops\"\)", text)
