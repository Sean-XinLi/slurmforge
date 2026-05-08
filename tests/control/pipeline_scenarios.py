from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.control.pipeline_helpers import read_json, stage_finished
from tests.control.pipeline_overlays import with_current_python


@dataclass(frozen=True)
class TrainEvalControlScenario:
    spec: Any
    plan: Any
    pipeline_root: Path
    client: Any


def build_train_eval_control_scenario(
    root: Path | str,
    *,
    extra: dict | None = None,
    client: Any | None = None,
) -> TrainEvalControlScenario:
    from tests.support.internal_records import materialize_train_eval_pipeline_for_test
    from tests.support.public import (
        compile_train_eval_pipeline_plan,
        load_experiment_spec,
        write_demo_project,
    )
    from tests.support.slurm import FakeSlurmClient

    project_root = Path(root)
    spec = load_experiment_spec(
        write_demo_project(project_root, extra=with_current_python(extra))
    )
    plan = compile_train_eval_pipeline_plan(spec)
    materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
    return TrainEvalControlScenario(
        spec=spec,
        plan=plan,
        pipeline_root=Path(plan.root_dir),
        client=client or FakeSlurmClient(),
    )


def submit_initial_and_complete_train_task(
    scenario: TrainEvalControlScenario,
    *,
    task_index: int = 0,
    state: str = "COMPLETED",
) -> str:
    from slurmforge.control.workflow import submit_initial_pipeline

    submit_initial_pipeline(scenario.plan, client=scenario.client)
    train_job_id = scenario.client.submissions[0].job_id
    complete_train_task(
        scenario,
        train_job_id,
        task_index=task_index,
        state=state,
    )
    return train_job_id


def complete_train_task(
    scenario: TrainEvalControlScenario,
    train_job_id: str,
    *,
    task_index: int,
    state: str = "COMPLETED",
) -> None:
    train_root = Path(scenario.plan.stage_batches["train"].submission_root)
    _run_stage_task(train_root, task_index=task_index)
    scenario.client.set_array_task_state(train_job_id, task_index, state)


def advance_train_completion(
    scenario: TrainEvalControlScenario,
    *,
    train_index: int = 0,
):
    from slurmforge.control.workflow import advance_pipeline_once

    return advance_pipeline_once(
        scenario.pipeline_root,
        hint=stage_finished(
            scenario.plan.stage_batches["train"].stage_instances[
                train_index
            ].stage_instance_id
        ),
        client=scenario.client,
        missing_output_grace_seconds=0,
    )


def complete_eval_from_workflow_state(
    scenario: TrainEvalControlScenario,
    *,
    eval_index: int = 0,
) -> None:
    workflow_state = read_json(
        scenario.pipeline_root / "control" / "workflow_state.json"
    )
    eval_submission = next(
        item
        for item in workflow_state["submissions"].values()
        if item["stage_name"] == "eval"
    )
    eval_root = Path(eval_submission["root"])
    eval_job_id = next(iter(eval_submission["groups"].values()))["scheduler_job_id"]
    _run_stage_task(eval_root, task_index=eval_index)
    scenario.client.set_array_task_state(eval_job_id, eval_index, "COMPLETED")


def advance_eval_completion(
    scenario: TrainEvalControlScenario,
    *,
    eval_index: int = 0,
):
    from slurmforge.control.workflow import advance_pipeline_once

    return advance_pipeline_once(
        scenario.pipeline_root,
        hint=stage_finished(
            scenario.plan.stage_batches["eval"].stage_instances[
                eval_index
            ].stage_instance_id
        ),
        client=scenario.client,
        missing_output_grace_seconds=0,
    )


def _run_stage_task(stage_root: Path, *, task_index: int) -> None:
    from tests.support.public import execute_stage_task

    exit_code = execute_stage_task(stage_root, 1, task_index)
    if exit_code != 0:
        raise AssertionError(
            f"stage task {stage_root}:{task_index} failed: {exit_code}"
        )
