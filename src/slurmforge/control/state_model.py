from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..storage.workflow import write_workflow_status
from ..workflow_contract import WORKFLOW_TERMINAL_STATES
from .control_submissions import submitted_control_records, submitted_control_job_ids
from .state_records import WorkflowState, workflow_state_to_dict


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
        stage_jobs[_submission_stage_key(submission)] = job_ids
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
        status,
        reason=reason,
        control_jobs=submitted_control_records(pipeline_root),
        stage_jobs=submitted_stage_job_ids_from_state(state),
        instances=workflow_state_to_dict(state)["instances"],
        dependencies=workflow_state_to_dict(state)["dependencies"],
        dispatch_queue=workflow_state_to_dict(state)["dispatch_queue"],
        submissions=workflow_state_to_dict(state)["submissions"],
        terminal_aggregation=workflow_state_to_dict(state)["terminal_aggregation"],
    )


def stage_key(stage_name: str, *, dispatch_id: str | None = None) -> str:
    return stage_name if dispatch_id is None else f"{stage_name}:{dispatch_id}"


def _submission_stage_key(submission) -> str:
    initial_id = f"{submission.stage_name}_initial"
    if submission.submission_id == initial_id:
        return submission.stage_name
    return stage_key(submission.stage_name, dispatch_id=submission.submission_id)
