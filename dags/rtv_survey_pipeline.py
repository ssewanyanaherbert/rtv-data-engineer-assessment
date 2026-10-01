"""RTV household survey pipeline: Bronze -> Silver -> Gold -> data tests -> report.

Each task shells out to ``python -m rtv_pipeline <step>`` so Spark runs in its own process
and every step can also be run by hand outside Airflow.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule

PROJECT_DIR = os.environ.get("RTV_PROJECT_DIR", "/opt/airflow/project")
CYCLES = ["baseline", "year1", "year2"]

log = logging.getLogger(__name__)


def _on_failure(context) -> None:
    ti = context["task_instance"]
    log.error("Task %s failed in run %s (try %s)", ti.task_id, context["run_id"], ti.try_number)


def step(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"python -m rtv_pipeline {command}",
        cwd=PROJECT_DIR,
        env={"PIPELINE_RUN_ID": "{{ run_id }}"},
        append_env=True,
        **kwargs,
    )


with DAG(
    dag_id="rtv_survey_pipeline",
    description="Three-cycle SurveyCTO household survey: lake ingest, conformance, warehouse, tests and report",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": _on_failure,
    },
    tags=["rtv", "survey", "spark", "postgres"],
    doc_md=__doc__,
) as dag:
    init = step("init_warehouse", "init")
    catalog = step("build_form_catalog", "catalog")
    bronze = [step(f"bronze_{cycle}", f"bronze --cycle {cycle}") for cycle in CYCLES]
    silver = step("build_silver", "silver")
    gold = step("build_gold", "gold")
    tests = step("run_data_tests", "test", retries=0)
    report = step("publish_report", "report", trigger_rule=TriggerRule.ALL_DONE)

    init >> catalog >> bronze >> silver >> gold >> tests >> report
