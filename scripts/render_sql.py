"""Print the project's SQL DDL for one environment (it never runs anything).

The files in sql/ are the production DDL. For dev they are rewritten so every `workspace.<layer>.` name becomes
`workspace.dev_<layer>.`. Review the output, then paste it into the Databricks SQL editor yourself.

Examples:
  python scripts/render_sql.py --env dev > dev_ddl.sql     (keep the output out of Git)
  python scripts/render_sql.py --env dev --file sql/10_bronze_tables.sql
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from taxilake.namespace import render_sql  # noqa: E402

# Setup order; sql/31_migrate_fact_row_hash.sql only migrates a pre-existing production table and is not rendered.
SQL_FILES = (
    "00_setup_catalog.sql",
    "10_bronze_tables.sql",
    "20_silver_tables.sql",
    "30_gold_tables.sql",
    "40_ops_tables.sql",
    "41_ops_pipeline_runs.sql",
)


def render_files(environment: str, catalog: str = "workspace", only: str | None = None) -> str:
    names = [Path(only).name] if only else list(SQL_FILES)
    parts = []
    for name in names:
        text = (ROOT / "sql" / name).read_text(encoding="utf-8")
        parts.append(f"-- ===== {name} ({environment}) =====\n{render_sql(text, catalog, environment)}")
    return "\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["prod", "dev"], default="dev")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--file", help="render a single file from sql/")
    args = parser.parse_args()
    sys.stdout.write(render_files(args.env, args.catalog, args.file))
    return 0


if __name__ == "__main__":
    sys.exit(main())
