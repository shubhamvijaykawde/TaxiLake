# Phase 5 — Asset Bundles and CI/CD

Scope: bundle `dev` and `prod` targets, PR checks, and a deployment workflow. **Done-check:** merging a PR deploys the intended bundle automatically, with no Databricks UI step. Setup instructions for you are in `docs/CICD.md`.

## Status: implemented and verified locally; the done-check is NOT yet demonstrated

As of 2026-10-25 nothing had run on GitHub. Since then the repository has commits including the CI workflows, and a deployed production job exists (observed read-only 2026-10-28: `taxilake_pipeline`, deployment kind BUNDLE, created by and running as a UUID-shaped identity consistent with the service principal, PAUSED, 0 runs, old definition). This repository holds no record of the GitHub workflow results and I have not seen them, so **the cloud done-check is not independently demonstrated**; `docs/RUNLOG.md` records only that the author confirmed it. The new job definition (with `environment = prod`) has not been deployed, and `bundle validate -t prod` has not been run with the real service-principal identity.

## Local verification (run 2026-10-25)

| Check | Result | Evidence |
|---|---|---|
| Lint and format | Pass | `ruff check .` all checks passed; `ruff format --check .` 34 files formatted |
| Workflow syntax | Pass | `actionlint` on `checks.yml`, `ci.yml`, `deploy.yml`: no findings |
| Tests | Pass | 111 passed (76 earlier + 35 new: bundle config, workflow guards, deployment safety gate) |
| Guard tests catch regressions | Pass | Four deliberate mutations each failed the suite: `pr-gate` removed from `needs`; prod schedule set to `UNPAUSED`; `bundle run` added to `ci.yml`; an unpinned action. Files were restored and re-verified identical |
| `bundle validate -t dev` | Pass | Validation OK |
| `bundle validate -t prod` | Pass with expected notes | Validation succeeds with a placeholder service-principal value; prod resolves to one job (`taxilake_pipeline`), `PAUSED`, `mode: production`, `run_as` a service principal. Locally it warns that the folder has a permission for my own user that the bundle does not list; in CI the deploying identity is the service principal listed in `permissions`, so this warning is expected only locally |
| `prod` requires the SP variable | Pass | Validating `prod` without the variable fails with "no value assigned to required variable" |
| Safety gate script | Pass | Unit tests cover safe, extra job, missing job, unpaused schedule, scheduled run, missing schedule. Run against the dev summary it correctly fails (five jobs) |
| Sensitive-string scan | Pass | No host, email, token or UUID in `databricks.yml`, workflows, or the 48 files that would be committed |

Side effect to know about: `bundle validate -t prod` creates an empty folder `.../.bundle/taxilake/prod/files` under the validating identity's home (it contains no files, no state, no job). It was created in my own workspace folder by local validation. At that time no production job existed and the workspace listed only the five `[dev ...]` jobs (a production job was observed later, see Status).

## Not verified (cannot be verified without GitHub and the service principal)

- That Free Edition lets you generate an OAuth secret for a service principal (the auth smoke step will show).
- That GitHub-hosted runners can reach the workspace API.
- Exact required-check names (`checks / test`, `checks / bundle-validate` expected).
- The `pr-gate` GitHub API call, the production environment approval, and the first real deployment.
- The folder ACL on the service principal's home folder (to be read after the first deploy, see below).

## Evidence required after you merge (Claude checks read-only)

1. You report the `deploy` workflow result (`checks`, `pr-gate`, `deploy` all green).
2. `databricks bundle summary -t prod` / `jobs get`: `taxilake_pipeline` exists under the service principal's root path, created by the service principal, `pause_status: PAUSED`, zero runs, and no other job in that bundle.
3. `databricks workspace get-status` and `databricks permissions get directories|jobs` show the ACLs; recorded in `docs/RUNLOG.md`.
4. Delta history of bronze/silver/gold/ops tables shows no commits after the Phase 4 reruns (CI did not touch data).
5. `ops.month_reconciliation` has no new rows.

## Safety properties (also enforced by tests)

`prod` has only `taxilake_pipeline`; the schedule is PAUSED; no workflow runs a job, SQL, upload or backfill; only `pull_request` and `push` to `main` trigger workflows; deployment needs same-commit checks, a merged PR, a kill-switch variable, an approved environment and a fresh tip of `main`; actions are pinned to commit SHAs; the token is read-only; credentials come only from secrets and are never printed.

## Assumptions, limits and decisions

- `prod` shares the existing `workspace.*` schemas with `dev` (user-approved; deviation from PLAN.md's "separate schemas"). A manual run of either target writes the same tables; the pipeline is idempotent. **Update (2026-10-28):** a `dev_` schema-prefix isolation was deployed to `dev` and verified for 2025-04 (production-named tables unchanged by two dev runs); the new job definition is not yet deployed to `prod`; see `docs/ISOLATION_CHECKLIST.md`.
- Auth: service-principal OAuth M2M preferred; OIDC federation is impossible on Free Edition (no account console); PAT is a fallback only.
- Cloud checks for Phases 1-4 were run by Claude via the CLI at the user's request; Phase 5's done-check depends on GitHub steps only the user can perform.

## Not started

(Historical, written 2026-10-25.) Phase 6 (Power BI report) was started afterwards and is recorded in `docs/PHASE6_CHECKLIST.md`; the deployment evidence listed above has not been recorded in this repository.
