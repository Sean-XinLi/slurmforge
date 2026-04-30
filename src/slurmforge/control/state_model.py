from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..storage.batch_registry import BatchRegistryRecord
from ..storage.runtime_batches import iter_runtime_batch_records
from ..storage.workflow import write_workflow_status
from ..submission.ledger import submitted_group_job_ids
from ..workflow_contract import WORKFLOW_TERMINAL_STATES
from .control_submissions import submitted_control_records, submitted_control_job_ids
from .state_records import WorkflowState, workflow_state_to_dict


@dataclass(frozen=True)
class PipelineAdvanceResult:
    pipeline_root: str
    state: str
    submitted_stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    submitted_control_job_ids: dict[str, str] = field(default_factory=dict)
    completed: bool = False


def submitted_stage_job_ids(pipeline_root: Path) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for record in iter_runtime_batch_records(pipeline_root):
        job_ids = submitted_group_job_ids(Path(record.stage_batch_root))
        if not job_ids:
            continue
        stage_jobs[_stage_job_key(record)] = job_ids
    return stage_jobs


def result_from_state(
    pipeline_root: Path, state: WorkflowState
) -> PipelineAdvanceResult:
    return PipelineAdvanceResult(
        pipeline_root=str(pipeline_root),
        state=state.state,
        submitted_stage_job_ids=submitted_stage_job_ids(pipeline_root),
        submitted_control_job_ids=submitted_control_job_ids(pipeline_root),
        completed=state.state in WORKFLOW_TERMINAL_STATES,
    )


def set_workflow_status(
    pipeline_root: Path,
    state: WorkflowState,
    status: str,
    *,
    reason: str,
) -> None:
    write_workflow_status(
        pipeline_root,
        status,
        reason=reason,
        control_jobs=submitted_control_records(pipeline_root),
        stage_jobs=submitted_stage_job_ids(pipeline_root),
        instances=workflow_state_to_dict(state)["instances"],
        dependencies=workflow_state_to_dict(state)["dependencies"],
        dispatch_queue=workflow_state_to_dict(state)["dispatch_queue"],
        submissions=workflow_state_to_dict(state)["submissions"],
        terminal_aggregation=workflow_state_to_dict(state)["terminal_aggregation"],
    )


def stage_key(stage_name: str, *, dispatch_id: str | None = None) -> str:
    return stage_name if dispatch_id is None else f"{stage_name}:{dispatch_id}"


def _stage_job_key(record: BatchRegistryRecord) -> str:
    return stage_key(record.stage_name, dispatch_id=record.dispatch_id or None)
