# Dev/prod data isolation (Phase 7 follow-up)

## Status: dev-side isolation VERIFIED in Databricks for 2025-04 (2026-10-28); new prod job definition deployed by CI (user-reported) and observed in Databricks (2026-10-30); access control NOT provided (naming only)

Implemented locally (269 tests), then deployed to the **dev** target only and exercised for 2025-04 twice. The evidence below was observed directly through the CLI and SQL Statement API (not user-reported). What it shows: the dev runs wrote only to `workspace.dev_*` and the production-oriented `workspace.*` tables were unchanged before and after both runs. What it does **not** show: access control (naming is not access control). *Update 2026-10-30:* the statements about the deployed prod job below ("still the old definition", "CI gate was not run") are historical; the new definition was later deployed through the CI workflow (user-reported) and observed in Databricks, see the done-check rows and `docs/RUNLOG.md`. See "Observed results" and "Open".

## Mapping

| | prod (names unchanged) | dev (new) |
|---|---|---|
| Schemas | `workspace.landing`, `bronze`, `silver`, `gold`, `ops` | `workspace.dev_landing`, `dev_bronze`, `dev_silver`, `dev_gold`, `dev_ops` |
| Landing volume | `workspace.landing.raw` (`/Volumes/workspace/landing/raw`) | `workspace.dev_landing.raw` (`/Volumes/workspace/dev_landing/raw`) |
| Selected by | bundle variable `data_env: prod` | bundle variable `data_env: dev` |

The job parameter `environment` (default `${var.data_env}`) is read by every notebook. Names are built only by `src/taxilake/namespace.py`. This is isolation by naming inside one catalog: it prevents accidental writes; it is not an access control (one user owns both namespaces; grants for the CI service principal are unverified).

## What changed

| Area | Change |
|---|---|
| `src/taxilake/namespace.py` (new) | `resolve(catalog, environment)` validates both values; `prod` gives no prefix, `dev` gives `dev_`; anything else raises. `render_sql` rewrites production DDL names for an environment. |
| Notebooks (`bronze_load`, `silver_transform`, `gold_build`, `checks`, `freshness_check`, `phase0_smoke_test`) | New `environment` widget, default `dev` (a hand-run notebook can never land in production by default); every table and the volume come from the namespace. Transformation logic is unchanged. |
| `src/taxilake/runlog.py` | The two Spark helpers take the fully qualified table name. |
| `databricks.yml` | Variable `data_env` (no default), `dev` / `prod` per target, job parameter `environment` on the pipeline job and the four dev helper jobs. The prod pipeline job now carries `environment = prod`; its schedule is still PAUSED. |
| `scripts/landing.py`, `upload_to_volume.py` | `volume_path` requires an environment; the uploader has `--env` (default `dev`). |
| `scripts/backfill.py` | Dev only. It passes `environment=dev` explicitly (a dev bundle deployed before this change has no such job parameter, so the run is rejected instead of writing to production-named tables) and queries the dev reconciliation table. |
| `scripts/render_sql.py` (new) | Prints the DDL for an environment (stdout only, runs nothing). `sql/*.sql` stays the production DDL. |
| `scripts/verify_deployment.py` | CI gate also fails if the deployed prod job's `environment` default is not `prod`. The workflow file is unchanged. |
| Power BI | The author's `.pbix` is unchanged and reads the production schemas. **Update (2026-10-29):** the text model source `powerbi/model/taxilake_model.tmdl` now has an `Environment` parameter (default `prod`, or `dev`) that selects `gold`/`ops`/`silver` or their `dev_` versions for a report rebuilt from the model script. This is read-only, so it adds no write path to production; it is covered by static tests and was applied in Power BI Desktop to a copy of the report with `Environment = dev` (user-reported, 2026-10-30; `docs/RUNLOG.md`). This is naming-based data separation of what Power BI reads, not an access control. |

## Guards and tests (local, passing)

`tests/test_namespace.py` (dev never equals prod for any layer, table or volume; bad catalog or environment raises; DDL rendering), `test_render_sql.py` (rendered dev DDL contains no production name), `test_no_hardcoded_names.py` (no literal `workspace.<layer>`, `/Volumes/workspace` or `{catalog}.<layer>` in notebooks, scripts or `src/`; every notebook uses the namespace and the safe widget default), `test_bundle_config.py` (per-target `data_env`, parameter wiring, no hard-coded environment), `test_verify_deployment.py` (deployed prod job must default to `prod`), updated `test_landing.py` and `test_notebook_instrumentation.py`.

## Existing data

`workspace.*` stays production. It already contains the Phase 4 history **and** the February/April Phase 7 dev-test rows (`ops.pipeline_runs`, `ops.failure_injection`, reconciliation). Nothing is deleted or altered.

## Cloud steps (the original procedure; steps 1 to 7, 9 and 10 have since been run, see "Observed results"; step 8 was not run)

Do not deploy prod. Use the `taxilake-dev` profile. (The original text said "do not backfill"; the dev backfills of 2025-04 were later explicitly authorised.)

1. **Baseline production (read-only SQL), save the output:** row count of every table in `workspace.bronze`, `silver`, `gold`, `ops`; the latest `ops.month_reconciliation` row per month (`bronze_rows`, `silver_accepted`, `quarantined`, `fact_rows`, `fact_key_hash_sum`, `fact_row_hash_sum`); `DESCRIBE HISTORY` latest version of each production table; `SELECT max(finished_at) FROM workspace.ops.pipeline_runs`.
2. **Render the dev DDL** and review it before running anything:
   ```powershell
   .\.venv\Scripts\python.exe scripts/render_sql.py --env dev > dev_ddl.sql
   ```
   Keep `dev_ddl.sql` out of Git. Check that it mentions only `workspace.dev_*` and `/Volumes/workspace/dev_landing/`.
3. **Apply it** in the SQL editor (creates the `dev_*` schemas, the dev volume and empty tables). Then `SHOW SCHEMAS IN workspace;` and repeat the step 1 baseline: production must be identical.
4. **Validate and deploy the dev bundle:**
   ```powershell
   databricks bundle validate -t dev -p taxilake-dev
   databricks bundle summary -t dev -p taxilake-dev -o json
   databricks bundle deploy -t dev -p taxilake-dev
   ```
   In the summary, the `taxilake_pipeline` parameter `environment` must be `dev`.
5. **One dev month only:**
   ```powershell
   $env:DATABRICKS_CONFIG_PROFILE = "taxilake-dev"
   python scripts/backfill.py --only 2025-04
   ```
   Expect `workspace.dev_ops.month_reconciliation` PASS with 3,970,553 bronze / 3,706,768 silver / 263,785 quarantined / 3,706,768 fact rows, four SUCCESS rows in `workspace.dev_ops.pipeline_runs` with run ids, and the files in `workspace.dev_landing.raw`.
6. **Production unchanged:** repeat the step 1 baseline. Counts, fingerprints, table versions and the max `finished_at` must equal the saved output.
7. **Dev idempotency:** run `python scripts/backfill.py --only 2025-04` again. Expect bronze 0 inserted, gold merge 0/0/0, identical fingerprints in a second `dev_ops.month_reconciliation` row.
8. **Optional dev canary:** `python scripts/backfill.py --only 2025-04 --inject-failure isolationcanary` (the marker lands in `workspace.dev_ops.failure_injection`, not in production's).
9. **Dev freshness:** run `notebooks/freshness_check.py` in the deployed dev copy with `environment = dev`; April should be FRESH and the others NEVER_RUN.
10. Confirm the production schedule is still PAUSED and no `prod` run exists.

## Observed results (2026-10-28, dev target only, profile `taxilake-dev`)

Commands run (all from the project root with `.venv/Scripts/python.exe`; nothing run against `prod`, no deploy by this step, no Git/GitHub):

- Read-only snapshots of `workspace.{bronze,silver,gold,ops}` and `workspace.dev_*` (a throw-away script outside the repo issuing only `SELECT`, `DESCRIBE HISTORY ... LIMIT 1` and `information_schema` queries through the Statement API; JSON saved under `data/isolation_baselines/`, git-ignored): `pre_run1`, `post_run1`, `post_run2`.
- `databricks jobs get` on the deployed dev and prod jobs (read-only).
- `.venv/Scripts/python.exe scripts/backfill.py --only 2025-04 --dry-run --profile taxilake-dev`, then `... --only 2025-04 --profile taxilake-dev` twice.
- One-time `databricks jobs submit` (a `SUBMIT_RUN`, not a job) of the deployed `freshness_check` notebook with `environment=dev`; the number of persistent jobs stayed 6.
- `databricks jobs list-runs --job-id <prod job>` (read-only).

| Check | Observed |
|---|---|
| Deployed dev job passes `environment` | `taxilake_pipeline` (dev copy) has parameter `environment` default `dev`; `job_run_id`/`task_run_id` base parameters present |
| Preflight | Dev tables all empty (0 rows) before run 1; dev volume held the April Parquet, zone CSV and weather JSONL; dry-run read 3,970,553 source rows |
| Run 1, batch `bf20261028T214506_202504`, job run `799990446134186` | All four tasks SUCCESS at attempt 1. `job_ms` 173,990; `wall_s` 190.4; command task times `silver_transform` 34,000, `checks` 20,000, `bronze_load` 54,000, `gold_build` 53,000 ms (command output) |
| Run 1 dev tables | `dev_bronze.yellow_trips` 3,970,553; `dev_silver.trips` 3,706,768; `dev_silver.trips_quarantine` 263,785; `dev_gold.fact_trips` 3,706,768; zones 265; weather 720; `dev_gold.dim_date` 30 |
| Run 1 reconciliation (`dev_ops.month_reconciliation`) | PASS; 0 orphans; 0 duplicates; key hash sum 15713211761864150056671; row hash sum 8296608893443640093870 |
| Run 1 `dev_ops.pipeline_runs` | 4 rows, all SUCCESS, attempt 1, `job_run_id` and `task_run_id` populated. `bronze_load` read 3,970,553 / written 3,970,553 (zones inserted 265, weather inserted 720); `silver_transform` 3,970,553 / 3,706,768 / quarantined 263,785 (reason hits 163 / 186,720 / 7 / 91,439); `gold_build` 3,706,768 / 3,706,768, MERGE inserted 3,706,768, updated 0, deleted 0; `checks` 3,970,553 / 1 / 263,785. `duration_s` (1 dp): 44.4, 28.5, 48.4, 14.3 |
| Production unchanged after run 1 | Snapshot `pre_run1` vs `post_run1`: **identical** (row counts, latest Delta version, operation and timestamp of every `bronze`, `silver`, `gold`, `ops` table; all 10 reconciliation rows; `ops.pipeline_runs` count 9, max `finished_at` 2026-10-28T15:53:35Z) |
| Run 2 (idempotency), batch `bf20261028T215151_202504`, job run `878695289751636` | All SUCCESS at attempt 1; `job_ms` 119,128; `wall_s` 138.0. `bronze_load` read 3,970,553 / written 0 (zones and weather inserted 0, table still 3,970,553); `silver_transform` identical counts and reason hits; `gold_build` read 3,706,768, MERGE inserted/updated/deleted 0/0/0; `checks` PASS. Dev table row counts unchanged; `dev_ops.pipeline_runs` 8 rows; `dev_ops.month_reconciliation` 2 rows with **identical** counts and **identical** key/row hash sums to run 1 |
| Production unchanged after run 2 | Snapshot `pre_run1` vs `post_run2`: **identical** |
| Why dev fingerprints differ from production's April | The hashes include `_source_file`: dev rows carry `dbfs:/Volumes/workspace/dev_landing/raw/...`, production rows `dbfs:/Volumes/workspace/landing/raw/...` (checked with `SELECT DISTINCT _source_file`). Counts match production's April row exactly; the production April fingerprints are key -9890027643591791769995 / row 19399707547174770864981. This difference is expected and is itself evidence that dev reads its own volume |
| Freshness (one-time submitted run, after run 1, before run 2) | overall NEVER_RUN (threshold 840 h); 2025-04 FRESH (last success 2026-10-28 21:47:58 UTC, age 0.05 h); 2025-01, 02, 03, 05, 06 NEVER_RUN; newest month loaded 2025-04 |
| Production schedule and history (read-only) | Deployed prod `taxilake_pipeline` schedule PAUSED; `list-runs` returned 0 runs. The deployed prod job is still the old definition (no `environment` parameter, no run-id base parameters). The dev copy is also PAUSED |

The February failure-injection canary (step 8) was deliberately not re-run.

## Done-check

| Check | Status |
|---|---|
| Namespace mapping and guards implemented, local tests pass | Done (269 tests) |
| Dev DDL applied; production unchanged afterwards | **Verified (observed)**: dev tables existed empty; production snapshot identical across both runs. (The baseline taken before the DDL was applied was collected by the user; Claude's snapshot starts after the DDL and the dev deploy) |
| Dev month loads only into `dev_*` and reconciles | **Verified (observed)** for 2025-04 |
| Production counts, fingerprints, versions unchanged after the dev runs | **Verified (observed)** for runs 1 and 2 (limit: latest history entry per table, counts and reconciliation rows; not a content hash of every row) |
| Dev idempotency | **Verified (observed)** for 2025-04 |
| Dev freshness | **Verified (observed)** |
| Production schedule PAUSED, no prod run | **Verified (observed, read-only)**: PAUSED, 0 runs. Only for the currently deployed (old) prod job |
| Production deployment of the new job definition | **Done through CI on 2026-10-30 (workflow success on `main` commit `83cbb43`: user-reported; resulting state observed read-only by Claude)**: same job id `801604815952818` updated in place, parameter `environment = prod` and run-id `base_parameters` present, `PAUSED`, 0 runs, creator = Run as = the workspace's service principal, 6 jobs in the workspace, production-named tables/records/landing files identical to the pre-PR baseline. Schedule stays PAUSED |
| CI gate (`verify_deployment.py` requires `environment == prod`) in a real deploy | **Passed in a real deployment (user-reported workflow success; the step output is not stored here).** Independently, the deployed state I observed satisfies each condition the gate checks (one job, schedule PAUSED, `environment` default `prod`, no scheduled run) |
| `bundle validate -t prod` with the real CI service-principal identity | **Passed (user-reported):** `checks / bundle-validate` on PR head `db6f498` and the deploy workflow's validate step, both authenticated as the service principal. The earlier placeholder validation remains non-evidence |
| Access-control isolation | **Not provided**: naming only |
| Clean-checkout reproduction of the April dev workflow | **Verified (reported by the author, 2026-10-30; batch `bf20261030T144309_202504`, job run `907493238503128`)**: an independent newcomer (the author's friend) deployed the `dev` bundle from a clean Git checkout (branch `readme-clean-test`, commit `79bf377`) and ran 2025-04 with reconciliation PASS; the target remained `workspace.dev_*` (bronze 3,970,553, accepted 3,706,768, quarantined 263,785 in `dev_ops`). It was a repeat run on existing dev April data in the reused workspace, tested at that commit only. Claude did not re-snapshot the production-named tables for this run, so it adds no new production-unchanged evidence beyond the 2026-10-28 snapshots. Not extended to other months, a new workspace, `prod` or access control |
| Months other than 2025-04 in dev | Not loaded (one month while developing) |

## Rollback

Code: you revert the change in Git. Dev data: `DROP SCHEMA workspace.dev_<layer> CASCADE` for the five dev schemas (dev-prefixed names only; you run it). Production objects are never altered by this change, so there is no production data rollback.

## Limits

- Naming isolation only; no grants were created or checked.
- The new prod job definition (`environment = prod`) reaches production only after a merge. *Update 2026-10-30:* it was deployed that way (user-reported) and observed in Databricks. Isolation remains by naming only: the service principal that owns the job has no verified Unity Catalog grants, so a manual production run would probably fail on permissions rather than write; this is untested and no production run was made.
- The deployment approval (the `production` environment reviewer) is not independent: the reviewer is the merging author and self-review is allowed. This does not affect data isolation, but it is a limit on the control that guards the production job definition.
- `sql/31_migrate_fact_row_hash.sql` is not rendered for dev (it migrates an old production table).
- Months 2025-01, 03, 05 and 06 are deliberately not loaded into dev (one month while developing).
