from __future__ import annotations

from pathlib import Path
from typing import Any

from ..control_paths import workflow_status_path, workflow_state_path
from ..io import SchemaVersion, read_json, utc_now, write_json
from ..plans.train_eval import TrainEvalPipelinePlan
from ..workflow_contract import (
    EVAL_STAGE,
    TRAIN_EVAL_PIPELINE_KIND,
    TRAIN_STAGE,
    WORKFLOW_PLANNED,
)


def default_workflow_state(plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    instances: dict[str, dict[str, Any]] = {}
    dependencies: dict[str, dict[str, Any]] = {}
    dispatch_queue: list[str] = []
    for stage_name in plan.stage_order:
        batch = plan.stage_batches[stage_name]
        for instance in batch.stage_instances:
            initial_state = "ready" if stage_name == TRAIN_STAGE else "planned"
            instances[instance.stage_instance_id] = {
                "stage_instance_id": instance.stage_instance_id,
                "stage_name": stage_name,
                "run_id": instance.run_id,
                "state": initial_state,
                "submission_id": "",
                "scheduler_job_id": "",
                "scheduler_array_task_id": "",
                "output_ready": False,
                "reason": "",
                "ready_at": "",
            }
            if initial_state == "ready":
                dispatch_queue.append(instance.stage_instance_id)

    if TRAIN_STAGE in plan.stage_batches and EVAL_STAGE in plan.stage_batches:
        train_by_run = {
            instance.run_id: instance.stage_instance_id
            for instance in plan.stage_batches[TRAIN_STAGE].stage_instances
        }
        for eval_instance in plan.stage_batches[EVAL_STAGE].stage_instances:
            upstream = train_by_run.get(eval_instance.run_id)
            if not upstream:
                continue
            downstream = eval_instance.stage_instance_id
            dependencies[f"{upstream}->{downstream}"] = {
                "upstream_instance_id": upstream,
                "downstream_instance_id": downstream,
                "condition": "success",
                "state": "waiting",
            }

    return {
        "schema_version": SchemaVersion.WORKFLOW_STATE,
        "pipeline_id": plan.pipeline_id,
        "pipeline_kind": getattr(plan, "pipeline_kind", TRAIN_EVAL_PIPELINE_KIND),
        "state": WORKFLOW_PLANNED,
        "current_stage": plan.stage_order[0] if plan.stage_order else "",
        "instances": instances,
        "dependencies": dependencies,
        "dispatch_queue": dispatch_queue,
        "submissions": {},
        "final_gate": {"state": "pending", "job_id": "", "reason": ""},
        "release_policy": getattr(plan, "release_policy", "per_run"),
    }


def write_initial_workflow_state(root: Path, plan: TrainEvalPipelinePlan) -> None:
    root = Path(root)
    write_json(root / "control" / "control_plan.json", plan.control_plan)
    write_json(workflow_state_path(root), default_workflow_state(plan))
    write_json(
        workflow_status_path(root),
        {"schema_version": SchemaVersion.WORKFLOW_STATUS, "state": "planned"},
    )
    events = root / "control" / "events.jsonl"
    events.parent.mkdir(parents=True, exist_ok=True)
    events.touch()


def read_workflow_status(pipeline_root: Path) -> dict[str, Any] | None:
    path = workflow_status_path(pipeline_root)
    if not path.exists():
        return None
    return read_json(path)


def write_workflow_status(pipeline_root: Path, state: str, **payload: Any) -> None:
    write_json(
        workflow_status_path(pipeline_root),
        {
            "schema_version": SchemaVersion.WORKFLOW_STATUS,
            "updated_at": utc_now(),
            "state": state,
            **payload,
        },
    )
