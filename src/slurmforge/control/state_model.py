from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..storage.workflow import write_workflow_status
from ..storage.workflow_state_models import WorkflowState
from ..storage.workflow_status_records import WorkflowStatusRecord
from ..workflow_contract import WORKFLOW_TERMINAL_STATES
from .control_submission_ledger import submitted_control_job_ids
from .workflow_status_projection import workflow_status_control_jobs


@dataclass(frozen=True)
class PipelineAdvanceResult:
    pipeline_root: str
    state: str
    submitted_stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    submitted_control_job_ids: dict[str, tuple[str, ...]] = field(default_factory=dict)
    completed: bool = False


def submitted_stage_job_ids_from_state(
    state: WorkflowState,
) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for submission in state.submissions.values():
        job_ids = {
            group.group_id: group.scheduler_job_id
            for group in submission.groups.values()
            if group.scheduler_job_id
        }
        if not job_ids:
            continue
        stage_jobs[submission.display_key] = job_ids
    return stage_jobs


def result_from_state(
    pipeline_root: Path, state: WorkflowState
) -> PipelineAdvanceResult:
    return PipelineAdvanceResult(
        pipeline_root=str(pipeline_root),
        state=state.state,
        submitted_stage_job_ids=submitted_stage_job_ids_from_state(state),
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
        WorkflowStatusRecord(
            state=status,
            updated_at="pending",
            reason=reason,
            control_jobs=workflow_status_control_jobs(pipeline_root),
            stage_jobs=submitted_stage_job_ids_from_state(state),
        ),
    )


def stage_key(stage_name: str, *, dispatch_id: str | None = None) -> str:
    return stage_name if dispatch_id is None else f"{stage_name}:{dispatch_id}"
