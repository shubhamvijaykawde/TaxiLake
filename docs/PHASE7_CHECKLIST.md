# Phase 7 — Observability and write-up

Scope: `ops.pipeline_runs` (per task attempt: rows read, written, quarantined, ids, timings, status), a pipeline freshness check, and a README that a stranger can follow with every number traceable to a recorded run.

## Status: implemented; verified in the DEVELOPMENT workspace for two months (user-reported); done-check partly open

Implemented and unit-tested locally (193 tests). The user then verified it in Databricks through the `dev` target for 2025-04 and 2025-02 (results below, recorded in `docs/RUNLOG.md`). Nothing was deployed to or run in `prod`. The February/April results were produced while `dev` and `prod` still shared the `workspace.*` tables (see "Dev and prod share data" below). **Update (2026-10-28):** dev-side isolation was later verified for 2025-04 (two dev runs wrote only to `workspace.dev_*`; the production-named tables were unchanged), see `docs/ISOLATION_CHECKLIST.md`. Production deployment of the new job definition is not done. That is separate from the observability results.

## Cloud-observed evidence (dev target, user-reported 2026-10-28)

| Item | Evidence | Status |
|---|---|---|
| `ops.pipeline_runs` exists and receives rows | Rows for both batches were queried | **Verified** (user-reported) |
| 2025-04, batch `bf20261028T154424_202504`, job run `1122394466844971` | Four tasks SUCCESS at attempt 1; bronze 3,970,553; silver 3,706,768; quarantined 263,785; fact 3,706,768; duplicates 0; orphans 0; reconciliation PASS | **Verified** |
| Run ids populated | `job_run_id` and `task_run_id` filled for all 4 (April) and all (February) task records; `{{job.run_id}}` / `{{task.run_id}}` resolve | **Verified** |
| 2025-02 failure then retry, batch `bf20261028T155026_202502`, job run `805336210195363` | `silver_transform` attempt 1 FAILED: `RuntimeError: injected failure for test 'phase7canary' (first attempt)`; attempt 2 SUCCESS; other tasks SUCCESS at attempt 1; bronze 3,577,543; silver 3,311,525; quarantined 266,018; fact 3,311,525; duplicates 0; orphans 0; PASS | **Verified** |
| Counts match the earlier Phase 4 runs | Same figures as the Phase 4 rows for both months | **Verified** |
| Freshness report | 2025-02 FRESH; 2025-04 FRESH; 2025-01, 03, 05, 06 NEVER_RUN; overall NEVER_RUN; threshold 840 h; newest month loaded 2025-04 | **Verified**; the overall NEVER_RUN is the expected result (four months have no rows in the new table), not a failed check |
| Per-task `rows_read`, `rows_written`, `rows_quarantined` | April: `bronze_load` 3,970,553 / 0; `silver_transform` 3,970,553 / 3,706,768 / 263,785; `gold_build` 3,706,768 / 0; `checks` 3,970,553 / 1 / 263,785. February: `bronze_load` 3,577,543 / 0; `silver_transform` attempt 1 FAILED (rows_read 3,577,543), attempt 2 3,577,543 / 3,311,525 / 266,018; `gold_build` 3,311,525 / 0; `checks` 3,577,543 / 1 / 266,018. Each from the SQL query output (user-reported) | **Verified** (user-reported) |
| 2025-04 already-loaded expectation (`bronze_load` rows_read 3,970,553, rows_written 0) | Matches the April row above | **Verified** (user-reported) |
| April task timings | Backfill command output: `bronze_load` 67,000 ms, `silver_transform` 53,000 ms, `gold_build` 79,000 ms, `checks` 25,000 ms; `job_ms` 238,219; `wall_s` 267.3. Not proof that `pipeline_runs.duration_s` holds these values | Recorded as **command output only** |
| `pipeline_runs.duration_s` | April (s): `bronze_load` 55.818, `silver_transform` 46.626, `gold_build` 73.555, `checks` 19.988. February (s): `bronze_load` 12.722, `silver_transform` attempt 1 FAILED 5.007, attempt 2 SUCCESS 34.312, `gold_build` 54.058, `checks` 17.427. Read-only query, user-reported. Separate from the April backfill timings above, which measure a different reported quantity | **Verified** (user-reported) for these two batches |
| `pipeline_runs.details` | April: `bronze_load` zones/weather inserted 0, `yellow_table_rows_after` 24,083,384; `silver_transform` duplicates 0, hits DROPOFF_BEFORE_PICKUP 163 / NEGATIVE_FARE 186,720 / PICKUP_OUTSIDE_MONTH 7 / ZERO_DISTANCE 91,439; `gold_build` fact_rows_for_month 3,706,768, merge inserted/updated/deleted 0; `checks` fact_rows and silver_accepted 3,706,768, duplicates 0, PASS. February: `bronze_load` as April; `silver_transform` attempt 1 FAILED: **`details` NULL**; attempt 2 duplicates 0, hits DROPOFF_BEFORE_PICKUP 93 / NEGATIVE_FARE 182,655 / PICKUP_OUTSIDE_MONTH 31 / ZERO_DISTANCE 99,771; `gold_build` fact_rows_for_month 3,311,525, merge 0/0/0; `checks` fact_rows and silver_accepted 3,311,525, duplicates 0, PASS. `yellow_table_rows_after` = 24,083,384 is the whole table size after the run, not the month's rows | **Verified** (user-reported) for these two batches |
| February whole-job runtime (`job_ms`, `wall_s`) | Recorded from the local backfill log `data/backfill_log.jsonl` for batch `bf20261028T155026_202502`: `job_ms` 191,656; `wall_s` 211.7 (read 2026-10-29, values copied unchanged; the log entry was written by the user's backfill command, the run itself is not independently re-measured) | **Recorded** (see `docs/RUNLOG.md`) |
| Live production schedule PAUSED, no prod run history | Read-only check 2026-10-28: deployed prod `taxilake_pipeline` PAUSED, 0 runs (the old definition then). Re-observed 2026-10-30 after the CI deployment: PAUSED, 0 runs, new definition (`environment = prod`) | **Verified** (observed) |
| `bundle validate -t prod` with the real CI service-principal identity | Earlier validation used a literal placeholder; not evidence for the real identity. On 2026-10-30 the user reported the CI `checks / bundle-validate` job and the deploy workflow's validate step passed with the real identity (`docs/RUNLOG.md`) | **Passed** (user-reported) |
| Months 2025-01, 03, 05, 06 in `ops.pipeline_runs` | No rows in the production-named `ops.pipeline_runs`; overall freshness stays NEVER_RUN. Dev now has its own tables (`workspace.dev_*`), so a dev run no longer touches them (verified for 2025-04 only); only 2025-04 is loaded in dev | **Open** (not loaded; no recommendation to backfill the production-named tables) |
| Power BI TMDL development validation (`Environment = dev`, applied to a separate copy of the existing `.pbix`; reported 2026-10-30) | The user reported that the TMDL applied in Power BI Desktop, `SchemaPrefix` resolved to `dev_`, the native queries targeted `workspace.dev_gold` and `workspace.dev_silver`, the refresh succeeded, the April 2025 dev data and report values were as expected, Revenue MoM behaved as expected with March absent, and the original `.pbix` was unchanged by the user's hash check. Individual values, screenshots and hashes were not supplied or independently checked. Details: `docs/RUNLOG.md` | **Complete** (user-reported manual validation) |
| Power BI six-month development validation | Dev holds only 2025-04, so the six-month totals in `powerbi/validation/expected_values.md` cannot be compared; a rebuild of the report from a blank model using the script is also untested | **Open** |
| Dev-side observability re-check on `workspace.dev_*` (2025-04, two runs, observed 2026-10-28) | Four SUCCESS rows per run with ids; run 2 wrote 0 rows in bronze and gold; freshness 2025-04 FRESH, others NEVER_RUN; production-named tables unchanged. See `docs/ISOLATION_CHECKLIST.md` | **Verified** (observed) |

## Dev and prod share data (not an observability result)

The February and April tests used the `dev` target. Both targets pass `catalog = workspace`, and the schemas and volume (`workspace.bronze|silver|gold|ops`, `workspace.landing.raw`) are literals in notebooks, SQL and scripts, so `dev` and `prod` have the same data destinations. The tests therefore wrote to the shared `workspace.*` tables (`silver`/`gold` slices for 2025-02 and 2025-04 were rewritten idempotently, and the same tables feed Power BI). No production deployment or run occurred. This describes the state when those tests ran. The `dev_`-prefixed isolation was afterwards deployed to `dev` and verified for 2025-04 (`docs/ISOLATION_CHECKLIST.md`): dev runs now write to `workspace.dev_*` and left the production-named tables unchanged. The earlier February/April rows remain in the production-named tables and were not removed. Details and hard-coded locations are in `docs/RUNLOG.md`.

## What was implemented

| Item | Where |
|---|---|
| Table DDL (idempotent `CREATE TABLE IF NOT EXISTS`) | `sql/41_ops_pipeline_runs.sql` |
| Row building, attempt numbering, success/failure logging rules, Spark append helpers | `src/taxilake/runlog.py` |
| Freshness and coverage logic | `src/taxilake/freshness.py` |
| Notebook instrumentation (`run.guard()` around the work, `run.succeed(...)` last) | `notebooks/bronze_load.py`, `silver_transform.py`, `gold_build.py`, `checks.py` (transformation code unchanged) |
| Read-only freshness report | `notebooks/freshness_check.py` (not part of the job) |
| Run ids passed to the four pipeline tasks | `databricks.yml` (`base_parameters` `job_run_id` = `{{job.run_id}}`, `task_run_id` = `{{task.run_id}}`; names checked against the Databricks docs page on dynamic value references, not tested live; `sync.exclude` unchanged) |
| README rewrite | `README.md` |

### Columns of `ops.pipeline_runs`

`source_month`, `batch_id`, `task_key`, `attempt`, `job_run_id`, `task_run_id`, `started_at`, `finished_at`, `duration_s`, `status` (`SUCCESS` or `FAILED`), `rows_read`, `rows_written`, `rows_quarantined`, `details` (JSON), `error_message`. NULL means not applicable, never zero.

| Task | rows_read | rows_written | rows_quarantined | details |
|---|---|---|---|---|
| `bronze_load` | rows in the landed month Parquet file | rows `COPY INTO` inserted into `yellow_trips` | NULL | yellow rows after, zones and weather inserted |
| `silver_transform` | bronze rows of the month | accepted rows | quarantined rows | duplicates removed, hits per reason code |
| `gold_build` | silver rows of the month | `MERGE` inserted + updated | NULL | inserted, updated, deleted, fact rows |
| `checks` | bronze rows of the month | 1 (the reconciliation row) | quarantined rows | reconciliation status, silver accepted, fact rows, duplicates |

### Failure handling

- The SUCCESS row is written after all data work and assertions. If that write fails, the task raises (`RunLogError`) and the retry repeats the idempotent work, so success is never reported without a record.
- For `silver_transform`, `rows_written + rows_quarantined + duplicates_removed = rows_read` must hold before a SUCCESS row is written; otherwise the task fails.
- A failing task writes a FAILED row (best effort, with the counts known so far and the error text) and re-raises the original exception; if the FAILED write fails, a warning is printed and the original error still propagates.
- `attempt` = 1 + existing rows for the same month, batch and task (retries reuse the batch id). Unresolved or missing run ids are stored as NULL with a warning and never fail a task.
- Failures before the run record exists (invalid widget values, a missing `ops.pipeline_runs` table at start) are not logged; a missing table fails the task at the success write with a clear error. **Apply `sql/41_ops_pipeline_runs.sql` before deploying these notebooks.**
- The log is append-only and separate from the data tables, so it cannot change idempotency of the data or the reconciliation rows.

### Freshness definition

Per expected month (default 2025-01 to 2025-06): the **last complete successful run** is a batch in which the latest attempt of all four tasks is SUCCESS and `ops.month_reconciliation` has a PASS row for the same batch. States: `FRESH` (age since the last task finished is at most the threshold, default 35 days), `STALE`, `FAILED_LAST_RUN` (a task failed in the latest batch; the earlier success is still reported), `INCOMPLETE_LAST_RUN` (a task or the PASS reconciliation is missing), `NEVER_RUN`. The overall state is the worst month. **This is pipeline freshness only.** The newest month loaded is reported as coverage, never as freshness, because the input is a fixed historical range. With the schedule paused the state will become STALE over time; that is expected and honest.

## Local validation (2026-10-28)

| Check | Result |
|---|---|
| `ruff check`, `ruff format --check` | clean, 50 files formatted |
| `pytest` (full suite) | 193 passed |
| New tests | `tests/test_runlog.py` (success row, NULL vs zero, retry attempt, failed task logs and re-raises the same exception, error after the success row adds no FAILED row, log write failure on the success path fails the task, on the failure path does not mask the original error, silver count balance, run-id cleaning, DDL and Spark schema match the column list), `tests/test_freshness.py` (fresh, stale and the threshold boundary, never run, failed last run after an earlier success, retry within a batch, rerun after failure, mixed batches, reconciliation not PASS, empty input, worst-state rule, naive timestamps, months outside the range, report wording), `tests/test_notebook_instrumentation.py` (each notebook logs its own task key, one success record written last, run-id widgets, job passes the run ids, freshness notebook is read-only) |
| Not run locally | the notebooks themselves and the Spark append (run in the dev workspace by the user, see above). `databricks bundle validate -t dev -p taxilake-dev` was run later on 2026-10-28 and passed; `-t prod` was not re-run after the `base_parameters` change |

## Manual verification in Databricks (steps used; dev results are recorded above, step 6 and the per-task values remain open)

1. Create the table (SQL editor): contents of `sql/41_ops_pipeline_runs.sql`.
2. Validate and deploy the dev bundle:
   ```powershell
   databricks bundle validate -t dev -p taxilake-dev
   databricks bundle validate -t prod -p taxilake-dev   # with BUNDLE_VAR_run_as_service_principal set to a placeholder, as in Phase 5
   databricks bundle deploy -t dev -p taxilake-dev
   ```
3. Run one month and then the failure canary, with the batch id the driver generates:
   ```powershell
   $env:DATABRICKS_CONFIG_PROFILE = "taxilake-dev"
   python scripts/backfill.py --only 2025-04
   python scripts/backfill.py --only 2025-02 --inject-failure phase7canary
   ```
4. Query the log:
   ```sql
   SELECT source_month, batch_id, task_key, attempt, status, rows_read, rows_written, rows_quarantined, duration_s, job_run_id
   FROM workspace.ops.pipeline_runs ORDER BY finished_at;
   ```
   Expected: four SUCCESS rows (attempt 1) for the 2025-04 batch; for the canary batch a FAILED `silver_transform` row with `error_message` starting `RuntimeError: injected failure`, then a SUCCESS row with `attempt = 2`; `job_run_id` and `task_run_id` filled (if they are NULL, the parameter references were not resolved and the task still succeeded). For 2025-04, which is already loaded, `bronze_load.rows_written` should be 0 and `rows_read` 3,970,553; `silver_transform` should show `rows_written` 3,706,768 and `rows_quarantined` 263,785 (the values recorded in `docs/RUNLOG.md`).
5. Run `notebooks/freshness_check.py` in the workspace (read-only). Expect FRESH for the months that have a complete run since deployment, and NEVER_RUN for months with no `pipeline_runs` rows yet (the Phase 4 runs happened before this table existed), so the overall state is NEVER_RUN until all six months are re-run.
6. Re-check that the production schedule is still PAUSED and that no `prod` job run exists.

## Done-check

| Done-check | Status |
|---|---|
| `ops.pipeline_runs` logs rows read, written and quarantined per task | **Met for the two dev months (user-reported):** rows read, written and quarantined, attempts, status, errors and run ids are logged per task. `duration_s` and `details` (NULL on the FAILED attempt) are also verified for these two batches |
| Freshness check | **Met for dev, two months:** the report ran and showed FRESH for 2025-02 and 2025-04 and NEVER_RUN for the rest (expected); an overall FRESH state is not available while four months have no rows, and no further backfill is recommended while the targets share data |
| README a stranger could follow, every number from a recorded run | **PASSED for an independent newcomer on a clean Git checkout (reported by the author, 2026-10-30):** the author's friend, who was new to the project, followed the README's development steps from a fresh clone (branch `readme-clean-test`, commit `79bf377`) and completed them: Python environment and dependencies, 273 passing tests, Ruff checks, Databricks authentication, six-month dry run, development bundle deployment, April 2025 development backfill, reconciliation query, task run-log query and freshness notebook (`docs/RUNLOG.md`, batch `bf20261030T144309_202504`). **Qualifications:** the tester reused the existing Databricks workspace, development schemas, volume and tables, so provisioning a new workspace is untested; the April run was a repeat on loaded data, not a first load; the test applies to commit `79bf377`, and README and documentation edits made after that commit were not independently tested; the six-month dev backfill, the Power BI six-month comparison and a blank-report rebuild are separate open items. Numbers in the README are copied from `docs/RUNLOG.md` and labelled |
| Six-month backfill in the dev namespace | **Open:** only 2025-04 is loaded in `workspace.dev_*`; the six-month run has been exercised only as a dry run (source preflight, user-reported) |
| Production isolation | **Dev-side verified (observed 2026-10-28, 2025-04 only):** dev writes go to `workspace.dev_*`, production-named tables unchanged (`docs/ISOLATION_CHECKLIST.md`). **Update 2026-10-30:** the new job definition was deployed to `prod` by the CI workflow (`deploy` run for `main` commit `83cbb43`, user-reported) and observed read-only (same job, `environment = prod`, PAUSED, 0 runs; `docs/RUNLOG.md`). **Not verified:** access control (naming only). Not part of this phase's done-check. No production job was run |

## Assumptions and limits

- Phase 4 history has no `pipeline_runs` rows; only runs after deployment are logged.
- `rows_read` for `bronze_load` costs one extra Parquet count per month.
- Dynamic job-parameter names came from the Databricks documentation and were then confirmed live in dev: run ids were populated (user-reported).
- `dev` helper jobs also log (their notebooks are the same).
- Phase 5 config changed in one place only: `databricks.yml` `base_parameters` on the four pipeline tasks. It changes the job definition `prod` deploys on the next merge; the schedule stays PAUSED. If you do not want that, remove the four `base_parameters` blocks and the guard test `test_pipeline_job_passes_the_run_ids_to_every_task`; the run ids then stay NULL.

## Not started

No further phase exists in `docs/PLAN.md`.
