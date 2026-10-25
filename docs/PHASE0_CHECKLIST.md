# Phase 0 — Local Setup and Free-Tier Feasibility

Complete the account and software steps yourself. Do not paste tokens, passwords, or secret values into Claude Code.

## 1. Create and open the Databricks workspace

- [x] Sign up for Databricks Free Edition and sign in once in the browser. (Evidence: authenticated CLI profile reaches the workspace.)
- [x] Open the workspace and keep its workspace URL available for the CLI login command. (Evidence: `taxilake-dev` profile valid.)
- **Standing project constraint (not a one-time acceptance item):** do not create paid resources. Stop if a prompt asks for a paid plan or billing setup. Billing status has not been inspected or verified.

## 2. Install local prerequisites on Windows

Install Python **3.12**, Java **17**, Git, the Databricks CLI, and Power BI Desktop. Use official sources:

- Python 3.12: https://www.python.org/downloads/windows/ (choose the latest maintained Python 3.12.x 64-bit Windows installer; do not install 3.14/3.15 for this project).
- Java 17 (Eclipse Temurin): https://adoptium.net/temurin/releases/?version=17
- Git for Windows: https://git-scm.com/install/windows
- Databricks CLI: https://learn.microsoft.com/azure/databricks/dev-tools/cli/install (WinGet is supported on Windows).
- Power BI Desktop (64-bit): https://learn.microsoft.com/power-bi/fundamentals/desktop-get-the-desktop

For the Databricks CLI, PowerShell or Command Prompt:

```powershell
winget search databricks
winget install Databricks.DatabricksCLI
```

Restart the terminal after installation. If WinGet cannot find the package, use the official install page rather than an unofficial download.

## 3. Verify installations

Open a **new PowerShell** window and run:

```powershell
py -3.12 --version
java -version
git --version
databricks -v
```

Confirm Python reports 3.12.x and Java reports 17.x. **Recorded deviation:** Java is OpenJDK 21.0.12, kept deliberately (see `docs/RUNLOG.md`). If `py -3.12` fails, fix the installation/PATH before continuing. Open Power BI Desktop once to confirm it starts.

## 4. Authenticate the Databricks CLI yourself

Use the workspace URL from your browser. Run:

```powershell
databricks auth login --host https://YOUR-WORKSPACE-URL
```

Replace the placeholder locally; do not put a real workspace URL in public documentation. Complete the browser sign-in and accept a profile name. Then verify:

```powershell
databricks auth profiles -o text
databricks current-user me
```

The final command should return your authenticated identity. Never paste an access token into Claude Code. Recent CLI versions use browser-based OAuth and OS secure storage for the token.

## 5. Extract TaxiLake and initialise Git before adding application code

1. Extract the starter ZIP to a location you control, for example `C:\Users\YOUR-USER\Projects\TaxiLake`.
2. Open PowerShell in the extracted `TaxiLake` folder.
3. Verify `.gitignore`, `CLAUDE.md`, `docs\PLAN.md`, and `docs\RUNLOG.md` exist.
4. Initialise the local repository **before creating application code**:

```powershell
git init
git status --short
```

Do not commit or push yet unless you have personally reviewed the initial files. When you later create the GitHub repository, follow your normal Git workflow manually.

## 6. Phase 0 cloud checks (Claude Code can help draft; you must verify)

- [x] Read and approve Claude Code's plan-mode proposal before allowing edits. (Plan approved in session before any edits.)
- [x] In Databricks, run a minimal notebook cell to print the serverless Python version (verified 2026-10-25: Python 3.12.3, Spark 4.2.0, via `phase0_smoke_job`):

```python
import sys

print(sys.version)
```

- [x] Create a minimal Delta table and query it back (verified: `workspace.ops.smoke_test`, 3 rows, history v0).
- [x] Test whether Power BI Desktop can connect to the Free Edition SQL warehouse and display the test table. (User-confirmed 2026-10-25: Power BI Desktop connected to the Databricks SQL warehouse, loaded `workspace.ops.smoke_test` and displayed the three expected rows: bronze, silver, gold. Direct connection selected; no export fallback needed.) If not, record the export fallback decision; do not spend days troubleshooting the direct connection.
- [x] Validate and, if supported, deploy a minimal Declarative Automation Bundle and test job (verified: validate, deploy and run all succeeded on Free Edition).
- [x] Update `docs/RUNLOG.md` with actual results and the selected Power BI approach.
- [x] Do not start TLC ingestion until these checks are recorded. (All checks now recorded; no ingestion started. Phase 0 approved by the user; Phase 1 authorised.)

## Phase 0 acceptance criteria

- `databricks current-user me` works from your terminal.
- The test Delta table can be written and queried in the workspace.
- Power BI direct connectivity has been tested and either works or the export fallback is explicitly selected.
- Bundle/job support has been tested or its limitation is documented.
- Actual results, versions, and decisions are recorded in `docs/RUNLOG.md`.

## Verification commands used (executed 2026-10-25)

Files added for these checks: `sql/00_setup_catalog.sql`, `notebooks/phase0_smoke_test.py`, `databricks.yml`.

1. CLI identity:

```powershell
databricks current-user me -p taxilake-dev
```

2. Schema layout: open the SQL editor and run `sql/00_setup_catalog.sql`. It uses the built-in `workspace` catalog (fallback; no `lakehouse` catalog exists on Free Edition).

3. Bundle and Delta smoke test:

```powershell
databricks bundle validate -p taxilake-dev
databricks bundle deploy -t dev -p taxilake-dev
databricks bundle run phase0_smoke_job -t dev -p taxilake-dev
```

The job's `catalog` parameter defaults to `workspace`. Record the printed Python/Spark versions, the job outcome and its runtime from the run page.

4. Query it back in the SQL editor: `SELECT * FROM workspace.ops.smoke_test;` should return 3 rows.

5. Power BI Desktop (time-box to about an hour):
   - In Databricks: **SQL Warehouses → Serverless Starter Warehouse → Connection details**. Note the server hostname and HTTP path locally; do not commit them.
   - In Power BI Desktop: **Get data → Azure Databricks** (or **Databricks**). Paste the hostname and HTTP path, choose **Import**, and sign in with OAuth / your organisational account.
   - Load `workspace.ops.smoke_test` and show it in a table visual.
   - If it fails: select the export fallback (write gold tables as Parquet/CSV to a volume, download them with `databricks fs cp`, import into Power BI) and record the error message in `docs/RUNLOG.md`.

## Phase 0 status (2026-10-25): complete

| Acceptance criterion | Evidence | Source |
|---|---|---|
| `databricks current-user me` works | Returned an active user for profile `taxilake-dev` | Claude-run CLI, run log |
| Test Delta table written and queried | `workspace.ops.smoke_test`: 3 rows, history v0, read-back assertion passed | Job output and SQL Statement API |
| Power BI connectivity tested | Direct connection to the Serverless Starter Warehouse worked; 3 expected rows displayed | **User-confirmed** |
| Bundle/job support tested | `bundle validate`, `deploy`, `run` all succeeded; job SUCCESS in 57 s | CLI output, run log |
| Results recorded in `docs/RUNLOG.md` | Done | Run log |

Deviations (both recorded in the run log): the `workspace` catalog replaces `lakehouse` (none exists on Free Edition), and Java 21 is used instead of 17. "Do not create paid resources" is a standing project constraint, not an acceptance criterion. Phase 1 is not started.
