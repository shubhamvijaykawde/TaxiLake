# CI/CD setup (GitHub Actions + Databricks Asset Bundles)

Names only. Never put a secret value in this repository, in an issue, in a PR, or in chat.

## How it works

| Workflow | Trigger | What it does |
|---|---|---|
| `checks.yml` | called by the two below | `test`: ruff, ruff format check, actionlint, pytest (local Spark, Java 17). `bundle-validate`: auth smoke check + `bundle validate -t prod` |
| `ci.yml` | pull request into `main` | calls `checks.yml`. Never deploys, never starts a job |
| `deploy.yml` | push to `main` | calls `checks.yml` **again on the pushed commit**, then `pr-gate`, then `deploy` (validate, `bundle deploy -t prod`, safety gate). Never starts a job |

`deploy` runs only if: variable `DEPLOY_ENABLED` is `true`; `checks` passed on this exact commit; the commit belongs to a merged PR into `main`; the `production` environment is approved; and the commit is still the tip of `main`.

The `prod` target contains only the `taxilake_pipeline` job with its schedule **PAUSED**. It shares the existing `workspace.*` data with `dev` (decision recorded in `docs/RUNLOG.md`). CI/CD never runs jobs, SQL, uploads or backfills, so it cannot change data tables. The schedule must stay paused because landing runs on the laptop; a scheduled cloud run would have no files to load.

Production deploys to the CI service principal's own home folder: `/Workspace/Users/<service-principal-application-id>/.bundle/taxilake/prod`. Note that `bundle validate -t prod` itself creates an empty `files` folder under that root path.

## One-time setup (you)

1. **Databricks service principal.** Settings > Identity and access > Service principals > Manage > add one. Give it workspace access. Open it > Secrets > Generate secret (lifetime of your choice, up to 730 days). Copy the client ID and secret directly into GitHub; they are shown once.
   - If the workspace has no "Generate secret" option, tell Claude. Fallback: a short-lived personal access token (`DATABRICKS_TOKEN`); that needs a small workflow change and a `databricks.yml` change because `prod` currently runs as a service principal.
2. **GitHub repository** (public, created empty).
3. **First push, without workflows.** Commit everything except `.github/` and push to `main`: `git add . ':!.github'`. With no workflow on `main`, nothing can run.
4. **Secrets** (Settings > Secrets and variables > Actions > Secrets): `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`. They are secrets, not variables, so a public repository's logs mask them. Do **not** create the variable `DEPLOY_ENABLED` yet.
5. **Environment** `production` (Settings > Environments): deployment branches = `main` only; required reviewer = you.
6. **Workflow permissions** (Settings > Actions > General): read-only default token.
7. **Open a PR** from a branch that adds `.github/`. The `ci` workflow runs `checks`. Fix any failure.
8. **After the first green run:** protect `main` (Settings > Branches or Rulesets): require a pull request and the status checks `checks / test` and `checks / bundle-validate` (confirm the exact names shown by GitHub). Then create the variable `DEPLOY_ENABLED` = `true`.
9. **Merge the PR** (squash or merge commit) and approve the `production` deployment. The merge is the first deployment.

## Rotation and shutdown

- Rotate: generate a new secret on the service principal, update `DATABRICKS_CLIENT_SECRET`, delete the old secret in Databricks.
- Emergency stop: delete the variable `DEPLOY_ENABLED` (or set it to anything other than `true`). Deployments are skipped.
- Never enable the job schedule from CI. Re-enabling it is a deliberate manual change after the landing step is automated.
