# Phase 3 — Gold: star schema

Scope: 2025-01 only. Pure builders in `src/taxilake/gold.py`, run by `notebooks/gold_build.py` (job `phase3_gold_job`), writing `workspace.gold.*`.

## Commands (run 2026-10-25)

```powershell
.\.venv\Scripts\python.exe -m pytest -q
# run sql/30_gold_tables.sql (SQL editor or Statement API)
databricks bundle deploy -t dev -p taxilake-dev
databricks bundle run phase3_gold_job -t dev -p taxilake-dev --params "month=2025-01,batch_id=g1"
```

## Done-check results

| Done-check | Result | Evidence |
|---|---|---|
| Keys are unique | Pass | `fact_trips`: 3,254,520 rows, 3,254,520 distinct `trip_key`; no duplicate keys in dim_zone, dim_date, dim_time_of_day, dim_payment_type (SQL) |
| Every fact row finds its dimensions | Pass | LEFT JOIN orphan counts are 0 for pickup zone, dropoff zone, date, hour and payment type (SQL, independent of the notebook) |
| Gold totals reconcile with silver | Pass | Fact rows = silver rows = 3,254,520; total_amount, fare_amount, trip_distance and tip_amount sums are equal between fact and silver; both aggregate tables sum to 3,254,520 trips and the same revenue |
| Idempotent (MERGE) | Pass | Batch `g2`: 0 inserted, 3,254,520 updated, 0 deleted; counts and sums unchanged |
| pytest passes locally | Pass | 46 passed (32 earlier + 14 gold tests) |

## Assumptions, limits and decisions

- See the Phase 3 notes in `docs/RUNLOG.md` (key design, left join for weather, redundant updates on re-run, the failed first run).
- The redundant-update limitation noted here was resolved in Phase 4 (`row_hash` conditional MERGE; see `docs/RUNLOG.md`).
- Dimensions are static or Type 1 (overwritten), not slowly changing.
- Cloud checks were executed by Claude via the authenticated CLI profile; please re-verify before approving the phase.

## Not started

Phase 4 (orchestration and backfill of the full range). Wait for the user's review and confirmation of this done-check.
