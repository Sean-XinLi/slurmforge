from __future__ import annotations

from pathlib import Path

from ..control_paths import workflow_status_path, workflow_state_path
from ..io import SchemaVersion, read_json, utc_now, write_json
from ..plans.train_eval import TrainEvalPipelinePlan
from ..workflow_contract import (
    WORKFLOW_PLANNED,
)
from .workflow_state_factory import build_initial_workflow_state
from .workflow_state_models import WorkflowState
from .workflow_state_serde import workflow_state_to_dict
from .workflow_status_records import (
    WorkflowStatusRecord,
    workflow_status_from_dict,
    workflow_status_to_dict,
)


def default_workflow_state(plan: TrainEvalPipelinePlan) -> WorkflowState:
    return build_initial_workflow_state(plan)


def write_initial_workflow_state(root: Path, plan: TrainEvalPipelinePlan) -> None:
    root = Path(root)
    workflow_state = default_workflow_state(plan)
    workflow_state_payload = workflow_state_to_dict(workflow_state)
    write_json(root / "control" / "control_plan.json", plan.control_plan)
    write_json(workflow_state_path(root), workflow_state_payload)
    write_workflow_status(
        root,
        WorkflowStatusRecord(
            state=WORKFLOW_PLANNED,
            updated_at=utc_now(),
            control_jobs={},
            stage_jobs={},
        ),
    )
    events = root / "control" / "events.jsonl"
    events.parent.mkdir(parents=True, exist_ok=True)
    events.touch()


def read_workflow_status(pipeline_root: Path) -> WorkflowStatusRecord | None:
    path = workflow_status_path(pipeline_root)
    if not path.exists():
        return None
    return workflow_status_from_dict(read_json(path))


def write_workflow_status(pipeline_root: Path, record: WorkflowStatusRecord) -> None:
    write_json(
        workflow_status_path(pipeline_root),
        workflow_status_to_dict(
            WorkflowStatusRecord(
                schema_version=SchemaVersion.WORKFLOW_STATUS,
                state=record.state,
                updated_at=utc_now(),
                reason=record.reason,
                control_jobs=record.control_jobs,
                stage_jobs=record.stage_jobs,
            )
        ),
    )
