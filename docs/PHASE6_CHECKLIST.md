# Phase 6 — Power BI report

Scope: Import the gold star schema into Power BI, define relationships and DAX measures, build three pages, and add real screenshots to the README. Window: January to June 2025. Setup instructions: [`docs/POWERBI_SETUP.md`](POWERBI_SETUP.md).

## Status: complete for the public deliverables; the `.pbix` is intentionally local only

All four acceptance criteria are verified, with one scope change the user decided: the `.pbix` is not published (see below). Nothing here claims the `.pbix` is safe to publish.

Decision (user, 2026-10-28): the `.pbix` is **intentionally not published**. The public project provides the reusable TMDL model, DAX validation queries, expected values, report specification, setup guide and genuine screenshots. The original acceptance criterion "a real `.pbix` saved in the repository" is therefore met only as a local file; this is a deliberate change of scope for the public repository, not a silent one.

## Acceptance criteria

| Criterion | Status | Evidence / what is missing |
|---|---|---|
| A real `.pbix` | **Exists locally; intentionally not published** | `powerbi/TaxiLake.pbix`, 20,838,523 bytes (20.8 MB), git-ignored and excluded from bundle sync. The saved data model appears to retain the real Databricks hostname and HTTP path (see below), so neither the original nor the placeholder candidate is to be published |
| Gold star schema connected with validated relationships | **Met** | Model view screenshot (user-supplied, inspected 2026-10-28) shows 11 tables (10 data tables and the `_Measures` table with its measures) and 10 solid relationship lines: fact to date, hour, zone and payment type; hourly aggregate to date and hour; daily aggregate to date; date to `dim_month`; `dq_reconciliation` and `dq_quarantine_by_rule` to `dim_month`. Every line has `1` on the dimension side and `*` on the detail side, one filter-direction marker each, pointing from the dimension towards the detail table. `dim_date` shows the calendar icon on its `date` column, which is how Power BI marks the date-table column. Together with 0 orphan rows and correct splits in the DAX validation |
| Three report pages and the required DAX measures work | **Met** | 15 of 15 DAX validation checks matched `powerbi/validation/expected_values.md`. The three pages exist locally; the screenshots show the validated figures. Total Revenue, Trips, Average Fare, Revenue per Mile and Revenue MoM % are present |
| Actual screenshots saved and included in the README | **Met** | `docs/img/page1_demand.png`, `page2_revenue_payments.png`, `page3_data_quality.png` (renamed from `Demand.png`, `Revenue.png`, `Data_Quality.png`; content unchanged), embedded in the README. They show no hostname or account name |

## What was verified

| Check | Result |
|---|---|
| DAX validation | 15 of 15 matched (output pasted by the user, compared by me): row counts of the 10 data tables; 0 orphans; overall and monthly measures; MoM; payment type, borough, zone, hour, weather band; data quality per month, overall and by rule; aggregate-vs-fact differences within floating-point noise (8.3e-7); MoM guard blank |
| Model tables | 11 tables: 10 data tables plus the measure-only table `_Measures` |
| Refresh | All 11 model tables loaded in under 30 seconds: **as reported by the user**, not measured by me |
| Screenshots | Content inspected: values agree with the validated measures (22,215,869 trips; $626,084,920.56 revenue; $19.38 average fare; $4.35 revenue per mile; 7.75% quarantine rate; All PASS; Last Reconciliation Check 2026-10-25 18:11:30) |
| `check_pbix.py` on the original `.pbix` | Exit 1: a Databricks host and warehouse path in the saved TMDL script tab. The first run failed with `KeyError` (the newer file format has no `DataMashup` part); the script was fixed and now handles both formats |
| `check_pbix.py` on the placeholder candidate (kept outside the repository) | Exit 0: no connection values in the readable text parts; 20.8 MB. **This does not show the candidate is safe:** its compressed `DataModel` is byte-identical to the original's (the placeholders were saved only as pending changes in `UnappliedChanges`), so the real values very probably remain in the model. The user reopened it and confirmed the pages still show cached data (user-reported) |
| Local lint, format, tests (2026-10-28, final) | `ruff check` clean, 44 files formatted, 139 tests pass (includes the guard test for the bundle `sync.exclude` and the `.gitignore` entry) |
| `databricks bundle validate -t dev` (profile `taxilake-dev`) | Validation OK. The resolved bundle shows `sync.exclude` = `powerbi/*.pbix`, `docs/img/**` |
| `databricks bundle validate -t prod` (placeholder service-principal value via `BUNDLE_VAR_run_as_service_principal`, as in Phase 5) | Validation succeeds with the same one warning and one recommendation as in Phase 5 (the workspace folder has a CAN_MANAGE permission for the local user that the bundle does not list; expected locally, not in CI). No `deploy`, no job run, no SQL |

## Limits of the `.pbix` check (uncertainty that remains)

- The `DataModel` part is compressed (XPress9); neither `check_pbix.py` nor my scan can read it. The connection parameters live there, so the check cannot show that they are absent.
- `SecurityBindings` is not human-readable and cannot be fully interpreted.
- A clean result from the checker means "no host or warehouse path in the readable text parts", nothing more. **The `.pbix` is not proven safe to publish and is not to be described as such.**
- The optional experiment of applying placeholders into the model (which forces a refresh with invalid details and may empty the tables) was declined and not run.

## Local files (not to be published)

- `powerbi/TaxiLake.pbix` (original, with real connection values).
- `C:\Users\svkaw\Documents\TaxiLake_local_backup\` holds a backup of the original and the placeholder candidate, outside the project folder.

## Bundle sync

Done with user approval on 2026-10-28: `databricks.yml` now has a top-level `sync.exclude` of `powerbi/*.pbix` and `docs/img/**`, so a bundle deploy does not upload the local report or the screenshots to the workspace. No target, job, schedule, permission or variable was changed. `.gitignore` also lists `powerbi/*.pbix`. `tests/test_bundle_config.py` has a new guard test for both. Verified: `bundle validate -t dev` and `-t prod` both pass with the new `sync` key. Side effect: none new. `validate -t prod` uses the empty `.../.bundle/taxilake/prod` folder in the validating user's workspace home that Phase 5 validation already created (the CLI reported its existing permission); no file, job or state was added, and no deploy was run. Not verified: the behaviour of an actual deploy with the exclusion (no deploy was run, by design).

## Screenshot observations (cosmetic, no figure is wrong)

- Revenue page: the month-over-month table is sorted by `Revenue Prior Month`, so rows read 01, 02, 03, 05, 04, 06; sort it by `year_month` if the screenshot is retaken. The bottom slicer items are clipped.
- Data quality page: the last columns of "Rows processed per load" are cut off.
- Two screenshots show a thin sliver of Power BI's window chrome at the top edge.
- The Revenue page footer says revenue includes "refunds/adjustments"; the precise statement is that revenue is the sum of `total_amount`, which includes 1,592 trips with a negative total.

## Model view: what the screenshot does and does not establish

Established: 11 tables, 10 relationship lines, all solid (none dashed), `1`/`*` cardinality markers with `1` on the dimension side, one filter-direction marker per line, calendar icon on `dim_date[date]`. Limits: the direction markers are small, so the direction is read from their position and agrees with `taxilake_model.tmdl` (all single direction, enforced by a static test) rather than being read arrow by arrow; the screenshot shows the date-table icon but not the "Mark as date table" dialog.

## Remaining uncertainty

- The `.pbix` files are not proven safe to publish (compressed model and `SecurityBindings` not inspectable) and are not to be published.
- The refresh time is user-reported.
- The three screenshots have the cosmetic issues listed above; none affects a figure.

## Limits

Aggregated fact `fact_trips_hourly` (user-approved) instead of the trip-level table, so dropoff zone and per-trip detail are not available. Weather charts show associations only. "Freshness" means the last reconciliation timestamp, not a source feed. MoM for January is blank (no December 2024 data). Revenue includes 1,592 trips with a negative total.

## Not started

Phase 7 (observability and write-up). Do not start it until the Phase 6 done-check is verified.
