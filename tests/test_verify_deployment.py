import json

import pytest
from verify_deployment import check, load_json


def summary(jobs=("taxilake_pipeline",), status="PAUSED"):
    return {"resources": {"jobs": {name: {"schedule": {"pause_status": status}} for name in jobs}}}


def job(status="PAUSED", environment="prod"):
    return {
        "settings": {
            "schedule": {"pause_status": status},
            "parameters": [
                {"name": "catalog", "default": "workspace"},
                {"name": "environment", "default": environment},
            ],
        }
    }


def test_safe_deployment_passes():
    assert check(summary(), job(), []) == []
    assert check(summary(), job(), {"runs": []}, require_no_runs=True) == []


def test_extra_dev_job_in_prod_fails():
    problems = check(summary(jobs=("taxilake_pipeline", "phase1_bronze_job")), job(), [])
    assert any("exactly one job" in p for p in problems)


def test_missing_pipeline_job_fails():
    assert any("exactly one job" in p for p in check(summary(jobs=()), job(), []))
    assert any("exactly one job" in p for p in check(summary(jobs=("other",)), job(), []))


@pytest.mark.parametrize("status", ["UNPAUSED", None, "paused"])
def test_unpaused_schedule_fails_in_summary_and_deployed_job(status):
    assert any("bundle summary" in p for p in check(summary(status=status), job(), []))
    assert any("deployed job" in p for p in check(summary(), job(status=status), []))


def test_missing_schedule_fails():
    assert any("deployed job" in p for p in check(summary(), {"settings": {}}, []))


def test_scheduled_run_fails_but_manual_run_is_tolerated_by_default():
    assert any("schedule" in p for p in check(summary(), job(), [{"trigger": "PERIODIC"}]))
    assert check(summary(), job(), [{"trigger": "ONE_TIME"}]) == []


def test_require_no_runs_flags_any_run():
    problems = check(summary(), job(), [{"trigger": "ONE_TIME"}], require_no_runs=True)
    assert any("expected none" in p for p in problems)


def test_load_json_handles_bom_and_empty_output(tmp_path):
    bom = tmp_path / "a.json"
    bom.write_bytes(b"\xef\xbb\xbf" + json.dumps({"a": 1}).encode())
    empty = tmp_path / "b.json"
    empty.write_text("", encoding="utf-8")
    assert load_json(str(bom)) == {"a": 1}
    assert load_json(str(empty)) == []


@pytest.mark.parametrize("environment", ["dev", "", None, "PROD"])
def test_deployed_job_must_default_to_the_prod_environment(environment):
    assert any("environment" in p for p in check(summary(), job(environment=environment), []))


def test_deployed_job_without_an_environment_parameter_fails():
    legacy = {"settings": {"schedule": {"pause_status": "PAUSED"}, "parameters": [{"name": "catalog", "default": "x"}]}}
    assert any("environment" in p for p in check(summary(), legacy, []))
