# Phase 2 — Silver: clean and validate

Scope: 2025-01 only. Pure transformations in `src/taxilake/silver.py`, executed by `notebooks/silver_transform.py` (job `phase2_silver_job`), writing `workspace.silver.trips` and `workspace.silver.trips_quarantine`.

## Commands (run 2026-10-25)

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
# run sql/20_silver_tables.sql (SQL editor or Statement API)
databricks bundle deploy -t dev -p taxilake-dev
databricks bundle run phase2_silver_job -t dev -p taxilake-dev --params "month=2025-01,batch_id=s2"
```

## Done-check results

| Done-check | Result | Evidence |
|---|---|---|
| accepted + quarantined + duplicates removed = bronze rows | Pass | 3,254,520 + 220,706 + 0 = 3,475,226. Printed by the notebook assertion and re-checked by SQL against the tables; duplicate count of 0 confirmed independently with `SELECT DISTINCT` on bronze (3,475,226 distinct) |
| No row silently dropped | Pass | Every quarantine row has a non-empty `reason_codes` array (0 violations); the only removal path is the counted exact-duplicate step |
| Silver contains no rule violators | Pass | 0 rows in `silver.trips` with negative fare, non-positive distance, dropoff before pickup, null pickup, or pickup outside January; CHECK constraints present in table properties |
| Idempotent re-run | Pass | Batch `s3` gave identical counts; `_batch_id` is `s3` only (slice replaced) |
| pytest passes locally | Pass | 32 passed (15 Phase 1 + 17 silver), local PySpark 4.2.0 on Java 21, Windows; no fallback to CI was needed |

## Assumptions, limits and decisions

- See the Phase 2 notes in `docs/RUNLOG.md` (rules, missing `passenger_count`, NEGATIVE_FARE share, setup incident).
- Tests need `PYSPARK_PYTHON` set to the venv interpreter; `tests/conftest.py` does this (the default `python` on PATH differs).
- Weather and zones are not cleaned in silver in this phase; they are joined in Phase 3.
- Cloud checks were executed by Claude via the authenticated CLI profile; please re-verify before approving the phase.

## Not started

Phase 3 (gold). Wait for the user's review and confirmation of this done-check.
