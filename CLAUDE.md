# TaxiLake rules

- Read `docs/PLAN.md` before working. Implement **one phase only** per session and stop at its done-check.
- For each phase, start in plan mode: inspect the relevant plan section, propose a concise implementation plan, and wait for my approval before editing files.
- Free-tier only: Databricks Free Edition and free GitHub features. Do not introduce paid services or trial-only dependencies.
- Never run `git commit`, `git push`, or create a pull request. I review changes and perform all Git operations myself.
- Never hard-code tokens, passwords, client secrets, or workspace URLs. Use the authenticated Databricks CLI profile or environment variables. Never ask me to paste a token into chat, print secret values, or read credential files into output.
- Do not modify credentials or perform interactive account setup on my behalf. I perform sign-up, browser authentication, and approvals myself.
- Develop and validate on **one month of TLC data**. Do not begin the full historical backfill until Phase 4.
- Keep raw data and generated exports out of Git. Do not download or process the full date range before the plan permits it.
- Do not silently drop invalid rows. Route them to a quarantine table with a stable reason code. If exact duplicates are removed, count them and include them in reconciliation.
- Every metric or number in the README and CV entry must come from a recorded run in `docs/RUNLOG.md`; use `TBD` until measured.
- Do not claim a cloud check passed from static code review or local tests alone. Show me the exact commands and have me verify cloud-side done-checks.
- Explain every Spark or Delta Lake concept introduced that I might need to defend in an interview, in 2–3 clear sentences.
- Prefer simple, testable transformations and explicit schemas. Add unit tests for transformation logic and failure cases.
- Record assumptions, blockers, feature limitations, and fallback decisions. If a Free Edition capability is unavailable, stop and document the planned fallback rather than silently switching to a paid service.
- Do not start the next phase until I have reviewed the changes and confirmed the current phase's done-check.
