# Phase 4 — Orchestration and backfill

Scope: **2025-01 to 2025-06 only** (user-chosen range). Job `taxilake_pipeline` (bronze -> silver -> gold -> checks, retries, paused monthly schedule), driver `scripts/backfill.py`, per-month reconciliation in `workspace.ops.month_reconciliation`.

## Commands (run 2026-10-25)

```powershell
.\.venv\Scripts\python.exe -m pytest -q
# once: sql/40_ops_tables.sql, sql/31_migrate_fact_row_hash.sql (SQL editor or Statement API)
databricks bundle validate -t dev -p taxilake-dev
databricks bundle deploy -t dev -p taxilake-dev
$env:DATABRICKS_CONFIG_PROFILE = "taxilake-dev"
python scripts/backfill.py --only 2025-02 --inject-failure canary1     # canary with a controlled failure
python scripts/backfill.py --start 2025-03 --end 2025-06 --resume      # rest of the range
python scripts/backfill.py --only 2025-04                              # idempotency re-run (also 2025-01)
```

## Done-check results

| Done-check | Result | Evidence |
|---|---|---|
| The job backfills all months | Pass | 6 months reconciled PASS: bronze 24,083,384 = accepted 22,215,869 + quarantined 1,867,515 + duplicates 0; fact rows 22,215,869 = distinct keys; each month's bronze rows equal its local Parquet footer count; 0 orphans; 0 silver rows outside their month |
| A forced failure retries | Pass | Month 2025-02, test `canary1`: `silver_transform` attempt 0 FAILED with the injected `RuntimeError`, attempt 1 SUCCESS, job SUCCESS (Jobs API run 111037981014270) |
| A second run of a month changes nothing | Pass | 2025-04 and 2025-01 re-run: per-month gold fingerprints and silver/quarantine counts identical before/after; gold MERGE 0 inserted / 0 updated / 0 deleted; bronze `COPY INTO` added no commit; ops fingerprints show one distinct value per month across runs |
| Unit tests | Pass | 76 passed locally (month ranges, resume logic, schema-drift check, reconciliation failures, failure-injection truth table, row_hash, upload skip) |
| `bundle validate -t dev` | Pass | Validation OK |

## Independent re-verification (2026-10-25, re-queried)

Re-run now against the cloud: per-month reconciliation rows (all PASS, identity holds per month), totals (24,083,384 / 22,215,869 / 1,867,515 / 0), live bronze/silver/quarantine/fact counts by month, live bronze distinct-row count, fact key uniqueness and dimension orphan checks, aggregate and silver-to-fact agreement per month, fingerprint counts across runs, Delta histories, schedule state (`PAUSED`, all recorded runs `ONE_TIME`), local Parquet footer counts, `pytest` (76 passed) and `bundle validate -t dev`. Historical (not re-run): the job outputs themselves, including the February retry, which was re-read from the Jobs API.

## Assumptions, limits and decisions

- Details, per-month figures and incidents are in `docs/RUNLOG.md` (Phase 4 rows and notes).
- Monthly schedule is defined but PAUSED: landing runs on the laptop, so a scheduled cloud run has no data unless the month was uploaded first.
- `ops.month_reconciliation` is a minimal precursor of Phase 7's `ops.pipeline_runs`.
- `ADD COLUMN IF NOT EXISTS` is unsupported, so `sql/31_migrate_fact_row_hash.sql` is a run-once migration.
- Remaining performance limits: silver rewrites each month fully on every run, the gold MERGE still scans the month to compare hashes, `checks` recomputes de-duplication, and months run strictly one at a time (details in `docs/RUNLOG.md`).
- `row_hash` is added by the run-once `sql/31_migrate_fact_row_hash.sql`; fresh environments get it from `sql/30_gold_tables.sql`.
- The failed first `checks` run (decimal overflow in Python) and its fix are recorded in `docs/RUNLOG.md`.
- Not covered: months outside 2025-01..2025-06; behaviour when the daily compute quota is exhausted (never reached; the driver stops at the first failed month and `--resume` continues).
- Cloud checks were executed by Claude via the authenticated CLI profile; please re-verify before approving the phase.

## Not started

Phase 5 (Asset Bundles and CI/CD). Wait for the user's review and confirmation of this done-check.
