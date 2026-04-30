from __future__ import annotations

from pathlib import Path

from ..io import utc_now
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..workflow_contract import (
    WORKFLOW_BLOCKED,
    WORKFLOW_FAILED,
    WORKFLOW_SUCCESS,
)
from .notifications import submit_pipeline_terminal_notification
from .state_model import set_workflow_status
from .state_records import (
    INSTANCE_BLOCKED,
    INSTANCE_FAILED,
    INSTANCE_TERMINAL_STATES,
    WorkflowState,
)


def finalize_if_terminal(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> WorkflowState:
    if not state.instances:
        return state
    if not all(
        instance.state in INSTANCE_TERMINAL_STATES
        for instance in state.instances.values()
    ):
        return state
    terminal_state = _terminal_state(state)
    state.state = terminal_state
    state.current_stage = ""
    state.terminal_aggregation.state = "completed"
    state.terminal_aggregation.reason = _terminal_reason(state, terminal_state)
    state.terminal_aggregation.completed_at = utc_now()
    notification = submit_pipeline_terminal_notification(
        pipeline_root,
        plan,
        dependency_job_ids=_terminal_dependency_job_ids(state),
        client=client,
        max_dependency_length=max_dependency_length,
    )
    if notification is not None:
        state.terminal_aggregation.notification_job_ids = notification.scheduler_job_ids
    set_workflow_status(
        pipeline_root,
        state,
        terminal_state,
        reason=state.terminal_aggregation.reason,
    )
    return state


def _terminal_dependency_job_ids(state: WorkflowState) -> tuple[str, ...]:
    seen: set[str] = set()
    job_ids: list[str] = []
    for submission in state.submissions.values():
        for job_id in submission.scheduler_job_ids:
            if not job_id or job_id in seen:
                continue
            seen.add(job_id)
            job_ids.append(job_id)
    return tuple(job_ids)


def _terminal_state(state: WorkflowState) -> str:
    instance_states = [instance.state for instance in state.instances.values()]
    if any(item == INSTANCE_FAILED for item in instance_states):
        return WORKFLOW_FAILED
    if any(item == INSTANCE_BLOCKED for item in instance_states):
        return WORKFLOW_BLOCKED
    return WORKFLOW_SUCCESS


def _terminal_reason(state: WorkflowState, terminal_state: str) -> str:
    if terminal_state == WORKFLOW_SUCCESS:
        return "all stage instances completed successfully"
    failed = [
        instance.stage_instance_id
        for instance in state.instances.values()
        if instance.state == INSTANCE_FAILED
    ]
    if failed:
        return f"failed stage instances: {', '.join(sorted(failed))}"
    blocked = [
        instance.stage_instance_id
        for instance in state.instances.values()
        if instance.state == INSTANCE_BLOCKED
    ]
    return f"blocked stage instances: {', '.join(sorted(blocked))}"
