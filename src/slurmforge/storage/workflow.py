from __future__ import annotations

from pathlib import Path
from typing import Any

from ..control_paths import workflow_status_path, workflow_state_path
from ..io import SchemaVersion, read_json, utc_now, write_json
from ..plans.train_eval import TRAIN_EVAL_PIPELINE_KIND, TrainEvalPipelinePlan


def default_workflow_state(plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.WORKFLOW_STATE,
        "pipeline_id": plan.pipeline_id,
        "pipeline_kind": getattr(plan, "pipeline_kind", TRAIN_EVAL_PIPELINE_KIND),
        "state": "planned",
        "current_stage": plan.stage_order[0] if plan.stage_order else None,
        "train_groups": {},
        "final_gate": {"state": "pending"},
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
