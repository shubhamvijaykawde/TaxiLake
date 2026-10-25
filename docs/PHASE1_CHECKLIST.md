# Phase 1 — Landing and Bronze

Scope: one month (2025-01) of TLC yellow trips, the zone lookup, and Open-Meteo hourly weather, landed in `workspace.landing.raw` and loaded unchanged into `workspace.bronze` with metadata columns. Uses the Phase 0 `workspace` catalog deviation.

## Commands (all run 2026-10-25; the Windows venv is `.venv`)

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\download_sources.py --month 2025-01
.\.venv\Scripts\python.exe scripts\fetch_weather.py --month 2025-01
# run sql/10_bronze_tables.sql in the SQL editor (or via the SQL Statement API)
.\.venv\Scripts\python.exe scripts\upload_to_volume.py --month 2025-01 --profile taxilake-dev
databricks bundle deploy -t dev -p taxilake-dev
databricks bundle run phase1_bronze_job -t dev -p taxilake-dev --params "month=2025-01,batch_id=run1"
```

## Done-check results

| Done-check | Result | Evidence |
|---|---|---|
| Bronze row count equals the source file's row count | Pass | Source Parquet footer (pyarrow): 3,475,226 rows. `SELECT count(*) FROM workspace.bronze.yellow_trips` = 3,475,226 |
| Re-running the same month adds zero rows | Pass | Batch `run2`: `COPY INTO` inserted 0 rows into all three tables; counts before = after (3,475,226 / 265 / 744); all yellow rows still `_batch_id = run1` |
| Zones and weather loaded | Pass | 265 zone rows with no duplicate `LocationID`; 744 weather rows = 31 x 24, 2025-01-01T00:00 to 2025-01-31T23:00 |
| Unit tests | Pass | 15 passed (`pytest`, local): month parsing and rejection, URL/path building, leap-year bounds, weather flattening and misaligned-array failures |
| Metadata columns populated | Pass | `_source_file` (volume path), `_ingested_at` (3,475,226 non-null), `_batch_id` populated |

## Independent re-verification (2026-10-25, re-queried after the first write-up)

Re-run now: row counts (3,475,226 / 265 distinct / 744 across 31 days and 744 distinct hours), single `_source_file` and single `_batch_id = run1` for yellow and weather, `DESCRIBE HISTORY` on all three bronze tables (one `COPY INTO` commit each, no commit from run2), job run list (run2 and run1-success `SUCCESS`; earlier attempt `FAILED`), `pytest` (15 passed), `bundle validate` (OK), source Parquet footer (3,475,226). Scope: January 2025 only; no other month has been ingested.

## Assumptions, limits and decisions

- `COPY INTO` (not Auto Loader) was chosen for file-level idempotency without a checkpoint. Auto Loader remains an option for Phase 4.
- One month only; no full backfill.
- The first job attempt (run 380223274515671, FAILED after 86.8 s) failed with `DELTA_FAILED_TO_MERGE_FIELDS` on `LocationID` (zones CSV string vs INT column). It was fixed with an explicit cast; yellow had already loaded in that attempt, so the successful run reports 0 inserted for yellow. Table state was verified by query.
- Bronze `yellow_trips` contains pickups outside January (2024-12-31 to 2025-02-01); handled in Phase 2.
- Raw data lives in `data/landing/` and is gitignored.
- Cloud checks were executed by Claude through the authenticated CLI profile at the user's instruction; please re-verify before approving the phase.

## Not started

Phase 2 (silver). Per project rules, wait for the user to review and confirm this done-check first.
