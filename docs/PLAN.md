# TaxiLake Project Plan

Oct 25, 2026 · @shubh

## Goal

Build a medallion lakehouse on Databricks Free Edition: PySpark loads a real public dataset into bronze, silver and gold Delta tables, a scheduled job runs it incrementally, and Power BI reports on the gold layer. Everything is free and code lives in GitHub.

What it proves on the CV, filling the gaps FinBank and CiteShield leave:

- **Spark and Delta Lake**: PySpark transformations, MERGE upserts, schema enforcement, time travel
- **Medallion architecture and Unity Catalog**: catalog, schemas per layer, managed tables
- **Incremental processing**: Auto Loader or a watermark, idempotent re-runs, a backfill
- **Orchestration in the cloud**: a Lakeflow Job with task dependencies, retries and a schedule
- **Infrastructure as code and CI/CD**: Databricks Asset Bundles (check the current name in Phase 0) deployed from GitHub Actions
- **Data quality**: expectations on silver, a quarantine table (same idea as FinBank, new stack)
- **Power BI**: a star schema in gold, a semantic model with DAX measures, a 3-page report

## Stack and cost

Every tool below is free; you pay nothing beyond your existing Claude Pro. Databricks Free Edition gives serverless compute only, with a daily fair-use quota: if you exceed it, compute stops for the rest of the day, but data is kept ([limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). It is for non-commercial use, which a portfolio project is.

| Layer | Tool | Where it runs | Cost |
| --- | --- | --- | --- |
| Compute and storage | Databricks Free Edition (serverless, Unity Catalog, Delta) | Cloud | Free |
| Transformations | PySpark + Spark SQL | Databricks; local PySpark for unit tests | Free |
| Orchestration | Lakeflow Jobs | Databricks | Free |
| Deployment | Databricks CLI + Asset Bundles | Laptop and GitHub Actions | Free |
| Version control and CI | GitHub, GitHub Actions | Cloud | Free for public repos |
| Tests | pytest, chispa or plain DataFrame asserts | Laptop and CI | Free |
| BI | Power BI Desktop | Laptop (Windows) | Free |
| Coding | Claude Code, VS Code | Laptop | Claude Pro you already have |

The new laptop handles this easily: 16 GB RAM is enough for local PySpark tests and Power BI Desktop at the same time, and heavy compute runs in Databricks, not on your machine. The GPU is not needed. Install Java 17 for local PySpark; if the laptop runs Windows 11, Docker Desktop works too, though this project does not need it.

## Dataset

Use NYC Taxi and Limousine Commission (TLC) trip records: real, messy, published as monthly Parquet files, and big enough (about 3 million yellow-taxi trips a month) to justify Spark without breaking the free quota.

| Source | Format | Role |
| --- | --- | --- |
| Yellow taxi trips, 6 to 12 months | Monthly Parquet | Main fact data, loaded one month at a time to show incremental loads |
| Taxi zone lookup | CSV | Location dimension (borough, zone) |
| Open-Meteo historical weather for NYC | JSON API, no key | Second source, joined by hour, shows API ingestion |

The data has the problems a data engineer should handle: negative fares, zero-distance trips, timestamps outside the file's month, missing passenger counts and schema drift between years. Those become your silver-layer quality rules.

Landing: download each month on the laptop with a script, upload it to a Unity Catalog volume with the Databricks CLI. That avoids depending on outbound internet from serverless compute, and it mimics a real landing zone. Confirm the TLC download page and file names in Phase 0, since they occasionally change.

## Architecture

&#91;embedded content: lakehouse architecture · 4 layers, 1 job, CI/CD, Power BI\]

Rows that fail a rule leave silver for the quarantine table instead of disappearing; one parameterised job runs all layers, and GitHub Actions deploys it.

## Phased plan

Eight phases, about 3 to 4 weeks part-time. Each phase ends with a done-check you run yourself before telling Claude Code to start the next one. Develop on one month of data and switch to the full range only in Phase 4, to stay inside the daily quota.

0. **Setup and free-tier check** (1 to 2 days)
   - Create the Free Edition account, a catalog `lakehouse` with schemas `landing`, `bronze`, `silver`, `gold`, `ops`, and a volume for raw files. *Deviation (recorded in `docs/RUNLOG.md`): Free Edition has no `lakehouse` catalog, so the built-in `workspace` catalog is used with the same schema names and volume `workspace.landing.raw`; local Java is 21, not 17.*
   - Install the Databricks CLI, authenticate it, create the GitHub repo and a `CLAUDE.md` describing the project rules.
   - Spike the two risky parts before building anything: does Power BI Desktop connect to the Free Edition SQL warehouse, and do bundles deploy jobs?
   - Done when: `databricks current-user me` works, a notebook writes a test Delta table, and Power BI shows that table (or you have decided on the fallback in Risks).
1. **Landing and bronze** (2 to 3 days)
   - A Python script downloads monthly TLC Parquet files and the zone CSV and uploads them to the volume; a second script pulls hourly weather from Open-Meteo.
   - Load into bronze Delta tables with Auto Loader or `COPY INTO`, unchanged except for metadata columns: source file, ingestion time, batch id.
   - Done when: bronze row count equals the source file's row count, and re-running the same month adds zero rows.
2. **Silver: clean and validate** (3 to 4 days)
   - Cast types, standardise column names across years, drop exact duplicates, derive trip duration.
   - Quality rules (negative fare, zero distance, pickup outside the file's month, dropoff before pickup) send failing rows to `silver.trips_quarantine` with a reason code, never silently dropped.
   - Pure transformation functions live in a Python package so they can be unit-tested with local PySpark.
   - Done when: valid rows plus quarantined rows equal bronze rows, and pytest passes locally.
3. **Gold: star schema** (3 days)
   - `fact_trips` plus `dim_zone`, `dim_date`, `dim_time_of_day`, `dim_payment_type`; hourly weather attached to trips.
   - Load with `MERGE` on a trip key so re-runs are idempotent; add aggregate tables for the dashboard (daily revenue by borough, hourly demand).
   - Done when: keys are unique, every fact row finds its dimensions, and gold totals reconcile with silver.
4. **Orchestration and backfill** (2 days)
   - A Lakeflow Job with tasks bronze, silver, gold, checks; dependencies, retries and a monthly schedule.
   - A `month` parameter lets one job run backfill any month; backfill the full range here. *Decision (recorded in `docs/RUNLOG.md`): the backfilled range is 2025-01 to 2025-06 (6 months), driven from the laptop; the monthly schedule is defined but paused because landing is laptop-side.*
   - Done when: the job backfills all months, a forced failure retries, and a second run of a month changes nothing.
5. **Bundles and CI/CD** (2 to 3 days)
   - Define jobs and code as an Asset Bundle with `dev` and `prod` targets (separate schemas). *Decision (recorded in `docs/RUNLOG.md`): `prod` is code-only and shares the existing `workspace.*` data with `dev`; it deploys only `taxilake_pipeline` with its schedule paused. See `docs/CICD.md`.*
   - GitHub Actions: ruff, pytest with local Spark and bundle validate on every pull request; deploy to `prod` on merge to main.
   - Done when: a merged pull request deploys the job without you touching the UI.
6. **Power BI report** (2 to 3 days)
   - Import the gold star schema, set relationships, write DAX measures (revenue, trips, average fare, revenue per mile, month-over-month change).
   - Three pages: demand (time and zones), revenue and payments, data quality (quarantine rate by rule, rows per load).
   - Done when: the report opens from the repo's `.pbix`, and screenshots are in the README.
7. **Observability and write-up** (2 days)
   - An `ops.pipeline_runs` table logging rows read, written and quarantined per task, plus a freshness check.
   - README with architecture diagram, how to run, measured numbers (rows, runtimes, quarantine rate) and honest limitations, in the style of the FinBank README.
   - Done when: a stranger could follow the README, and every number in it comes from a recorded run.

## Working with Claude Code

Give Claude Code one phase at a time and verify before moving on, the same rhythm that worked for FinBank and CiteShield.

- Put this plan in the repo as `docs/PLAN.md` (export the doc as Markdown) and write a short `CLAUDE.md` with the hard rules: free tier only, no paid services, develop on one month of data, never hard-code tokens, quarantine instead of dropping rows, every number in the README must come from a recorded run.
- Start each phase with: "Read docs/PLAN.md and implement Phase N only. Stop after the done-check and show me the commands to verify it."
- Run the done-check yourself in Databricks before saying "Phase N works". Claude Code cannot see your workspace unless you paste results back.
- Keep tokens in environment variables or a `.databrickscfg` profile outside the repo, and add both to `.gitignore`.
- Ask Claude Code to explain any Spark or Delta concept it uses that you could not explain in an interview. Being able to defend the code matters more than finishing fast.

**GitHub is yours.** Claude Code writes and edits code only in your local folder; it never commits, pushes or creates pull requests. You review the changes, commit them and push them to GitHub yourself at the end of each phase. Add this rule to `CLAUDE.md` so every session follows it.

## Risks and fallbacks

| Risk | Fallback |
| --- | --- |
| Power BI cannot connect to the Free Edition SQL warehouse (auth or connector limits) | Export the gold tables to Parquet or CSV in a volume, download them with the CLI, and import into Power BI. The star schema and DAX work are the same; say so in the README. |
| Daily compute quota runs out | Develop on one month, keep tables partitioned by month, backfill one month per run. Data survives a quota stop, so you lose time, not work. |
| Service-principal OAuth secrets, or CI access to the workspace, are not available on Free Edition (no account console, so no OIDC federation) | Use a short-lived personal access token stored as a GitHub secret; if CI cannot reach the workspace at all, deploy from the laptop with the CLI and document it. |
| Asset Bundles or Jobs features are limited on Free Edition | Keep the job definition in the bundle anyway and deploy it with the CLI from your laptop; document what CI could not do. |
| TLC file names or schema change | The silver layer's column mapping per year handles drift; this is a feature to talk about in interviews. |
| Free Edition account deleted after long inactivity | All code is in GitHub and the pipeline rebuilds from raw files, so recreating the workspace costs an afternoon. Log in now and then. |
| Local PySpark on Windows fails (Java, winutils) | Run unit tests only in GitHub Actions (Linux), or test the pure-Python parts locally and the Spark parts in CI. |

## Draft CV entry

A target to build toward; replace every bracket with a measured number before using it.

TaxiLake: **NYC Mobility Lakehouse on Databricks**

- Built a **medallion lakehouse** (bronze, silver, gold **Delta Lake** tables in **Unity Catalog**) with **PySpark**, ingesting \[N\] million NYC taxi trips from monthly Parquet files plus hourly weather from a REST API.
- Implemented **incremental, idempotent loads** (Auto Loader, `MERGE` upserts) and **data-quality rules** that quarantine \[X\]% of rows with reason codes; orchestrated with **Lakeflow Jobs** including retries and a parameterised **backfill**.
- Deployed jobs as code with **Databricks Asset Bundles** and **CI/CD (GitHub Actions)**, with PySpark unit tests on every pull request.
- Modelled a **star schema** in the gold layer and built a **Power BI** report with DAX measures for demand, revenue and data quality.

Sources: [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [Power BI Databricks connector](https://learn.microsoft.com/en-us/power-query/connectors/databricks).
