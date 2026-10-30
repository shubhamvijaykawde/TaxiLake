"""Static checks that every pipeline notebook logs to ops.pipeline_runs the same way (they only run on Databricks)."""

import ast
import re
from pathlib import Path

import pytest
import yaml

from taxilake.runlog import TASK_KEYS

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = {task: ROOT / "notebooks" / f"{task}.py" for task in TASK_KEYS}


@pytest.mark.parametrize("task,path", NOTEBOOKS.items())
def test_notebook_parses_and_logs_its_own_task_key(task, path):
    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert re.search(rf'runlog\.TaskRun\(\s*month,\s*batch_id,\s*"{task}",', text), "wrong or missing task key"
    assert text.count("run.succeed(") == 1, "exactly one success record, written last"
    assert "run.guard()" in text
    for widget in ("job_run_id", "task_run_id"):
        assert f'dbutils.widgets.text("{widget}", "")' in text


@pytest.mark.parametrize("task,path", NOTEBOOKS.items())
def test_success_record_is_the_last_statement_of_the_notebook(task, path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    last = tree.body[-1]
    assert isinstance(last, ast.With), "the final block must be the guarded block ending in run.succeed"
    final_statement = last.body[-1]
    assert isinstance(final_statement, ast.Expr) and "run.succeed" in ast.unparse(final_statement)


def test_pipeline_job_passes_the_run_ids_to_every_task():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    tasks = bundle["resources"]["jobs"]["taxilake_pipeline"]["tasks"]
    assert [t["task_key"] for t in tasks] == list(TASK_KEYS)
    for task in tasks:
        assert task["notebook_task"]["base_parameters"] == {
            "job_run_id": "{{job.run_id}}",
            "task_run_id": "{{task.run_id}}",
        }


def test_pipeline_and_dev_helper_jobs_take_the_environment_from_the_target_variable():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    jobs = {"taxilake_pipeline": bundle["resources"]["jobs"]["taxilake_pipeline"]}
    jobs.update(bundle["targets"]["dev"]["resources"]["jobs"])
    for name, job in jobs.items():
        params = {p["name"]: p["default"] for p in job["parameters"]}
        assert params["environment"] == "${var.data_env}", name


def test_freshness_notebook_is_read_only_and_states_what_freshness_means():
    text = (ROOT / "notebooks" / "freshness_check.py").read_text(encoding="utf-8")
    ast.parse(text)
    assert "not source-data age" in text
    assert not re.search(r"\.write\.|INSERT|DELETE|MERGE|saveAsTable", text)
