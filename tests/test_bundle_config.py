"""Guard tests for databricks.yml: what production may contain and that nothing sensitive is committed."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
PROD = BUNDLE["targets"]["prod"]
DEV = BUNDLE["targets"]["dev"]
SENSITIVE = [
    r"https?://",
    r"\.cloud\.databricks\.com",
    r"dapi[0-9a-f]{16,}",
    r"[\w.+-]+@[\w-]+\.[\w.]+",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
]


def pipeline():
    return BUNDLE["resources"]["jobs"]["taxilake_pipeline"]


def test_only_the_pipeline_job_is_defined_for_every_target():
    assert set(BUNDLE["resources"]["jobs"]) == {"taxilake_pipeline"}
    assert set(PROD["resources"]["jobs"]) == {"taxilake_pipeline"}
    assert set(PROD["resources"]) == {"jobs"}


def test_phase_helper_jobs_exist_only_in_dev():
    helpers = {"phase0_smoke_job", "phase1_bronze_job", "phase2_silver_job", "phase3_gold_job"}
    assert helpers <= set(DEV["resources"]["jobs"])
    assert not helpers & set(PROD["resources"]["jobs"])


def test_schedule_is_paused_everywhere():
    assert pipeline()["schedule"]["pause_status"] == "PAUSED"
    assert PROD["resources"]["jobs"]["taxilake_pipeline"]["schedule"]["pause_status"] == "PAUSED"
    for name, job in DEV["resources"]["jobs"].items():
        assert (job.get("schedule") or {}).get("pause_status", "PAUSED") == "PAUSED", name
    assert "UNPAUSED" not in (ROOT / "databricks.yml").read_text(encoding="utf-8")


def test_prod_is_production_mode_with_a_service_principal_and_private_path():
    assert PROD["mode"] == "production"
    assert PROD["run_as"] == {"service_principal_name": "${var.run_as_service_principal}"}
    root_path = PROD["workspace"]["root_path"]
    assert root_path.startswith("/Workspace/Users/${workspace.current_user.userName}/")
    assert "/Shared" not in root_path
    assert PROD["permissions"] == [
        {"service_principal_name": "${var.run_as_service_principal}", "level": "CAN_MANAGE"},
    ]


def test_prod_variable_has_no_default_so_it_must_come_from_ci():
    assert "default" not in BUNDLE["variables"]["run_as_service_principal"]
    assert "run_as_service_principal" not in PROD.get("variables", {})


def test_every_task_of_the_pipeline_retries_and_is_ordered():
    tasks = {t["task_key"]: t for t in pipeline()["tasks"]}
    assert list(tasks) == ["bronze_load", "silver_transform", "gold_build", "checks"]
    assert all(t["max_retries"] == 2 for t in tasks.values())
    assert pipeline()["max_concurrent_runs"] == 1
    assert [d["task_key"] for d in tasks["checks"]["depends_on"]] == ["gold_build"]


def test_local_power_bi_report_and_screenshots_are_not_synced_or_committed():
    assert BUNDLE["sync"]["exclude"] == ["powerbi/*.pbix", "docs/img/**"]
    assert "powerbi/*.pbix" in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()


def test_no_host_token_email_or_uuid_in_the_bundle_file():
    text = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    for pattern in SENSITIVE:
        assert not re.search(pattern, text), pattern


@pytest.mark.parametrize("path", sorted((ROOT / ".github").rglob("*.yml")) or [None])
def test_no_host_token_or_email_in_workflows(path):
    if path is None:
        pytest.skip("no workflows present")
    text = path.read_text(encoding="utf-8")
    for pattern in SENSITIVE[1:4]:
        assert not re.search(pattern, text), (path.name, pattern)


def test_each_target_sets_its_own_data_environment_and_the_variable_has_no_default():
    assert "default" not in BUNDLE["variables"]["data_env"]
    assert DEV["variables"]["data_env"] == "dev"
    assert PROD["variables"]["data_env"] == "prod"
    assert DEV["mode"] == "development" and PROD["mode"] == "production"


def test_the_pipeline_job_takes_its_environment_from_the_variable_so_dev_cannot_inherit_prod():
    params = {p["name"]: p["default"] for p in pipeline()["parameters"]}
    assert params["environment"] == "${var.data_env}"
    assert params["catalog"] == "workspace"
    assert "environment" not in PROD["resources"]["jobs"]["taxilake_pipeline"].get("parameters", [])


def test_no_job_hard_codes_an_environment_value():
    jobs = [BUNDLE["resources"]["jobs"]["taxilake_pipeline"], *DEV["resources"]["jobs"].values()]
    for job in jobs:
        values = [p["default"] for p in job.get("parameters", []) if p["name"] == "environment"]
        assert values in ([], ["${var.data_env}"]), job.get("name")
    assert "environment" not in str(PROD["resources"]["jobs"]["taxilake_pipeline"].get("parameters", []))
