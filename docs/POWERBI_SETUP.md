# Power BI setup guide (Phase 6)

What is automated and what is not: the model (tables, queries, relationships, measures) is a script you paste into Power BI Desktop; the report pages are built by hand from `powerbi/REPORT_SPEC.md`. Nothing here changes Databricks: the two query tables run a read-only `SELECT`, and everything else reads tables as they are.

Never put the real server hostname or HTTP path in a file, an issue, a pull request, or chat. They stay in Power BI on your machine.

## 1. Get the connection details (once)

Databricks workspace > **SQL Warehouses** > **Serverless Starter Warehouse** > **Connection details**. You need **Server hostname** and **HTTP path** (you used both in Phase 0). The warehouse starts by itself when queried (Free Edition; auto-stops after 10 idle minutes).

## 2. Create the model

1. Open Power BI Desktop (Microsoft Store version 2.159 is installed). Start a **blank report**.
2. If you do not see **TMDL view** in the left strip: File > Options and settings > Options > Preview features > enable **TMDL view**, restart Desktop.
3. Open **TMDL view**, create a new tab, paste the whole content of `powerbi/model/taxilake_model.tmdl`, click **Apply**. If Desktop reports an error, copy the message (it contains no secrets) and send it to Claude; do not edit the script by guessing.
4. Home > **Transform data** > **Edit parameters**: set `ServerHostname` and `HttpPath` to your values; leave `Catalog` = `workspace`; choose `Environment`:
   - `dev` reads the schemas that `scripts/backfill.py` fills: `workspace.dev_gold`, `workspace.dev_ops` and `workspace.dev_silver`. **Choose this if you followed the README's "Reproduce it" steps**, because the backfill scripts never write to the production-named tables.
   - `prod` (the default in the script) reads `workspace.gold`, `workspace.ops` and `workspace.silver`, which hold the author's Phase 4 load. Choose it only if those tables exist in your workspace.

   A second, derived query `SchemaPrefix` turns `Environment` into the schema prefix (`dev_` or nothing); do not edit it. The choice is read at refresh time, so changing it and refreshing switches the whole model. OK, then **Close & Apply** / Refresh.

   *Status:* the parameter is covered by static tests (`tests/test_powerbi_model.py`: every table source goes through `SchemaPrefix`, and the resulting schema names equal the ones the pipeline writes). **Manual validation (user-reported, 2026-10-30; not independently observed):** the script was applied in Power BI Desktop to a separate test copy of an existing `.pbix`, with `Environment = dev`; `SchemaPrefix` resolved to `dev_`, the native queries targeted `workspace.dev_gold` and `workspace.dev_silver`, the model refreshed, and the April 2025 development values and the month-over-month measure (March absent in dev) were as expected. The original `.pbix` was unchanged by the user's hash check. **Still open:** a rebuild from a blank report using this guide, and a six-month comparison against `powerbi/validation/expected_values.md` (dev holds only 2025-04). For any new test, work on a copy of the `.pbix` and keep it outside the repository; applying the script resets `ServerHostname` and `HttpPath` to placeholders, so re-enter them before refreshing.
5. On first refresh sign in with the same method as Phase 0 (the Databricks connector, OAuth in the browser). Approve the **native database query** prompts for the two tables `fact_trips_hourly` and `dq_quarantine_by_rule` (they are `SELECT ... GROUP BY` statements, shown in the prompt).
6. Note the refresh duration and whether any table failed. Send both to Claude.

If a native query is rejected by the connector, do not change the model. Tell Claude: the fallback is to recreate those two queries through **Get data > Databricks** with the SQL statement in the advanced options (the two statements are the ones in the TMDL file), or, only with your explicit approval, to create one read-only view in the `gold` schema.

## 3. Check the model

1. **Model view**: expect 11 tables and 10 relationships, all many-to-one with the single arrow pointing from the dimension to the fact. `dim_month` sits between `dim_date` and the two data-quality tables. No dashed (inactive) lines. Send a screenshot if anything looks different.
2. Select `dim_date` > Table tools > **Mark as date table** with `date` as the key, if Desktop did not mark it already.
3. **DAX query view**: paste `powerbi/validation/validate_measures.dax` and run each query (or run all). Compare the results to `powerbi/validation/expected_values.md`. Send the 15 result tables to Claude (text copy or screenshot), who compares them to SQL-derived values. Do not proceed to the report pages until these match.

## 4. Build the report

Build the three pages exactly as listed in `powerbi/REPORT_SPEC.md` (about 45 to 60 minutes). Use `dim_month[year_month]` for every month slicer and sync it across pages.

## 5. Save and check the file

1. File > Save as `powerbi/TaxiLake.pbix` inside the project folder. This file stays local: the saved model keeps the real hostname and HTTP path you typed, so `powerbi/*.pbix` is git-ignored and excluded from bundle deployments. Do not publish it.
2. Run in a terminal in the project folder:

```powershell
.\.venv\Scripts\python.exe scripts\check_pbix.py powerbi\TaxiLake.pbix
```

It prints the file size and whether any readable part of the file contains a Databricks host or warehouse path (and, for older files, whether the Power Query script holds the placeholder parameters). It never prints the values. It cannot inspect the compressed data model, so a clean result is partial evidence and does not prove the file is safe to publish.

3. If it reports real values (Power BI keeps the values you typed), do not commit the file yet. Tell Claude. The options are an explicit exception from you, or an attempt to restore the placeholders on a copy and check whether the imported data survives; this is not verified yet.
4. If the size is above 100 MB, GitHub rejects the push without Git LFS. Tell Claude.

## 6. Screenshots

Capture the three pages as listed at the end of `powerbi/REPORT_SPEC.md` into `docs/img/`. Then ask Claude to validate the screenshots, update the README, and record the results.

## Checklist of what you must do (everything else is done by Claude)

1. Paste the TMDL script, set the two parameters, sign in, approve the native queries, refresh, and report errors/timing.
2. Run the DAX validation queries and send the output.
3. Build the three pages, save `powerbi/TaxiLake.pbix`, run `check_pbix.py`, take three screenshots.
