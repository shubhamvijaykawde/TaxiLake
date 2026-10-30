# Phase 5 — Asset Bundles and CI/CD

Scope: bundle `dev` and `prod` targets, PR checks, and a deployment workflow. **Done-check:** merging a PR deploys the intended bundle automatically, with no Databricks UI step. Setup instructions for you are in `docs/CICD.md`.

## Status: implemented; done-check met on 2026-10-30 (GitHub evidence user-reported, Databricks state observed)

As of 2026-10-25 nothing had run on GitHub (historical). **Update 2026-10-30:** the user reported that the required PR checks (`checks / test`, `checks / bundle-validate`) passed on PR head `db6f498`, that `bundle-validate` ran `bundle validate -t prod` with the real service-principal identity, and that the `deploy` workflow for `main` commit `83cbb43` then completed successfully (approval gate: `production` environment, user as required reviewer, `main` only, administrator bypass off). These GitHub results are **user-reported**; this repository does not store the workflow output. **Observed by Claude (read-only, 2026-10-30):** the production `taxilake_pipeline` kept its job id `801604815952818` (updated in place), now has `environment = prod` and the run-id `base_parameters`, is `PAUSED` with 0 runs, still has one creator and Run as identity equal to the workspace's service principal, and all production-named tables, records and landing files are identical to the baseline taken before the PR (details: `docs/RUNLOG.md`). Reading: a merged pull request deployed the intended bundle with no Databricks UI step. **Still not verified:** the approval step as seen in GitHub, the service principal's Unity Catalog grants (a manual production run would likely fail on permissions; none was made), access-control isolation, and the service principal's bundle folder contents. The production schedule stays PAUSED. Limitations that remain: unverified Unity Catalog grants for the service principal, naming-based (not access-control) isolation, and no independent deployment approval (the required reviewer is the merging author and self-review is allowed).

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

## Items that could not be verified before the first deployment (status after 2026-10-30)

Written on 2026-10-25 as "cannot be verified without GitHub and the service principal". Status now, with the evidence type for each (**user-reported** = reported by the author about GitHub or the Databricks UI; **observed** = read by Claude through the CLI/SQL API):

- That Free Edition lets you generate an OAuth secret for a service principal: **user-reported**. A secret created on 2026-10-26 exists under the service principal, and the CI workflows authenticated with it. Claude did not read the secret, its scope or its expiry.
- That GitHub-hosted runners can reach the workspace API: **user-reported** (the CI authentication smoke step and `bundle validate -t prod` passed on PR head `db6f498`, and the `deploy` workflow succeeded for `main` commit `83cbb43`).
- Exact required-check names: **user-reported** (`checks / test` and `checks / bundle-validate` are the required checks and passed).
- The `pr-gate` GitHub API call, the production environment approval and the first real deployment: **user-reported** workflow success for `83cbb43`. Claude did not see the workflow output or the approval step.
- The folder ACL on the service principal's home folder: **not read**. The job's own ACL was **observed** (service principal `IS_OWNER`, `admins` `CAN_MANAGE` inherited).

## Evidence required after the merge (status, 2026-10-30)

1. You report the `deploy` workflow result (`checks`, `pr-gate`, `deploy` all green): **user-reported** for `main` commit `83cbb43`. Claude also read the local git metadata: `83cbb43` is the merge commit of pull request #2, with parents `036b6b7` and `db6f498`, and its tree is identical to the tree of `db6f498` (observed from local git objects, so the deployed content equals the commit the PR checks ran on).
2. `taxilake_pipeline` exists, created by the service principal, `pause_status: PAUSED`, zero runs, no other `taxilake_pipeline`: **observed** with `jobs list` / `jobs get` / `jobs list-runs` (`bundle summary` was not run: it needs the service-principal identity).
3. `permissions get` on the job: **observed**. `workspace get-status` / directory ACLs: **not done**.
4. Delta history of the tables shows no commits after the earlier runs (CI did not touch data): **observed**, all 16 production-named tables identical to the pre-PR baseline.
5. `ops.month_reconciliation` has no new rows: **observed** (10 records unchanged; `ops.pipeline_runs` 9 records unchanged; landing volume unchanged).

Limitations that remain after the deployment: (1) the service principal's Unity Catalog grants on `workspace.*` are not verified, so a manual production run would probably fail on permissions (no run was made); (2) data isolation is by naming only, not an access control; (3) the deployment approval is **not independent**: the required reviewer is the same person who merges and self-review is allowed, so the gate is a deliberate pause, not a separation of duties.

## Safety properties (also enforced by tests)

`prod` has only `taxilake_pipeline`; the schedule is PAUSED; no workflow runs a job, SQL, upload or backfill; only `pull_request` and `push` to `main` trigger workflows; deployment needs same-commit checks, a merged PR, a kill-switch variable, an approved environment and a fresh tip of `main`; actions are pinned to commit SHAs; the token is read-only; credentials come only from secrets and are never printed.

## Assumptions, limits and decisions

- `prod` shares the existing `workspace.*` schemas with `dev` (user-approved; deviation from PLAN.md's "separate schemas"). A manual run of either target writes the same tables; the pipeline is idempotent. **Update (2026-10-28):** a `dev_` schema-prefix isolation was deployed to `dev` and verified for 2025-04 (production-named tables unchanged by two dev runs); **Update (2026-10-30):** the new job definition was deployed to `prod` by the CI workflow (user-reported) and observed in Databricks (read-only); see `docs/ISOLATION_CHECKLIST.md` and `docs/RUNLOG.md`.
- Auth: service-principal OAuth M2M preferred; OIDC federation is impossible on Free Edition (no account console); PAT is a fallback only.
- Cloud checks for Phases 1-4 were run by Claude via the CLI at the user's request; Phase 5's done-check depends on GitHub steps only the user can perform.

## Not started

(Historical, written 2026-10-25.) Phase 6 (Power BI report) was started afterwards and is recorded in `docs/PHASE6_CHECKLIST.md`; the deployment evidence is recorded in `docs/RUNLOG.md` (2026-10-30 Phase 5 closeout row); the GitHub parts are user-reported.
