"""Guard tests for the GitHub Actions workflows: triggers, gating, pinning and what they must never do."""

import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
NAMES = ["checks", "ci", "deploy"]
FORBIDDEN = [
    "bundle run",
    "run-now",
    "jobs submit",
    "fs cp",
    "api post",
    "statements",
    "auto-approve",
    "set -x",
    "secrets: inherit",
    "pull_request_target",
    "workflow_dispatch",
    "schedule:",
    "pause_status",
    "UNPAUSED",
]


def load(name):
    text = (WORKFLOWS / f"{name}.yml").read_text(encoding="utf-8")
    return yaml.safe_load(text), text


def triggers(doc):
    return doc.get("on", doc.get(True))  # PyYAML parses a bare `on:` key as boolean True


def steps(doc, job):
    return doc["jobs"][job]["steps"]


def all_steps(doc):
    return [s for job in doc["jobs"].values() for s in job.get("steps", [])]


@pytest.fixture(scope="module", autouse=True)
def _workflows_exist():
    missing = [n for n in NAMES if not (WORKFLOWS / f"{n}.yml").exists()]
    assert not missing, f"missing workflows: {missing}"


@pytest.mark.parametrize("name", NAMES)
def test_least_privilege_and_no_forbidden_constructs(name):
    doc, text = load(name)
    assert doc["permissions"] == {"contents": "read"}
    assert ": write" not in text
    for pattern in FORBIDDEN:
        assert pattern not in text, (name, pattern)


@pytest.mark.parametrize("name", NAMES)
def test_external_actions_are_pinned_to_full_commit_shas(name):
    doc, _ = load(name)
    for step in all_steps(doc):
        uses = step.get("uses")
        if uses and not uses.startswith("./"):
            assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", uses), uses


@pytest.mark.parametrize("name", NAMES)
def test_checkouts_do_not_persist_credentials_and_secrets_are_never_echoed(name):
    doc, _ = load(name)
    for step in all_steps(doc):
        if step.get("uses", "").startswith("actions/checkout@"):
            assert step["with"]["persist-credentials"] is False
        assert "secrets." not in step.get("run", "")


def test_triggers_are_only_pull_request_push_to_main_and_workflow_call():
    assert triggers(load("ci")[0]) == {"pull_request": {"branches": ["main"]}}
    assert triggers(load("deploy")[0]) == {"push": {"branches": ["main"]}}
    assert set(triggers(load("checks")[0])) == {"workflow_call"}


def test_ci_and_deploy_call_the_same_reusable_checks_workflow():
    ci, _ = load("ci")
    deploy, _ = load("deploy")
    assert ci["jobs"]["checks"]["uses"] == "./.github/workflows/checks.yml"
    assert deploy["jobs"]["checks"]["uses"] == "./.github/workflows/checks.yml"
    assert set(ci["jobs"]) == {"checks"}


def test_checks_run_lint_tests_and_production_bundle_validation():
    doc, text = load("checks")
    assert set(doc["jobs"]) == {"test", "bundle-validate"}
    runs = " ".join(s.get("run", "") for s in all_steps(doc))
    for command in ["ruff check .", "ruff format --check .", "actionlint", "pytest", "bundle validate -t prod"]:
        assert command in runs, command
    assert "head.repo.full_name == github.repository" in doc["jobs"]["bundle-validate"]["if"]
    assert doc["jobs"]["bundle-validate"]["env"]["DATABRICKS_AUTH_TYPE"] == "oauth-m2m"
    assert "bundle deploy" not in text


def test_deploy_is_gated_on_same_commit_checks_pr_merge_kill_switch_and_environment():
    doc, text = load("deploy")
    jobs = doc["jobs"]
    assert set(jobs) == {"checks", "pr-gate", "deploy"}
    assert set(jobs["deploy"]["needs"]) == {"checks", "pr-gate"}
    assert jobs["deploy"]["environment"] == "production"
    for job in ("checks", "pr-gate"):
        assert "vars.DEPLOY_ENABLED == 'true'" in jobs[job]["if"]
    assert "commits/${SHA}/pulls" in text and "merged_at != null" in text
    assert steps(doc, "deploy")[0]["with"]["ref"] == "${{ github.sha }}"
    runs = " ".join(s.get("run", "") for s in steps(doc, "deploy"))
    assert "git rev-parse HEAD" in runs and "GITHUB_SHA" in runs
    assert "bundle validate -t prod" in runs and "bundle deploy -t prod" in runs
    assert "verify_deployment.py" in runs
    assert doc["concurrency"]["cancel-in-progress"] is False
    assert jobs["pr-gate"]["permissions"] == {"contents": "read", "pull-requests": "read"}


def test_databricks_connection_values_come_only_from_secrets_not_variables():
    for name in NAMES:
        _, text = load(name)
        assert "vars.DATABRICKS" not in text
        for key in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
            for line in text.splitlines():
                if re.match(rf"\s+{key}: ", line):
                    assert "${{ secrets." + key + " }}" in line, line


def test_only_the_deploy_workflow_deploys():
    for name in ("checks", "ci"):
        assert "bundle deploy" not in load(name)[1]
