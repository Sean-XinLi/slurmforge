from __future__ import annotations

from .workflow_state_models import (
    DependencyState,
    DispatchSubmissionState,
    StageInstanceState,
    WorkflowState,
    dependency_key,
)


def set_instance(state: WorkflowState, record: StageInstanceState) -> WorkflowState:
    state.instances[record.stage_instance_id] = record
    return state


def set_dependency(state: WorkflowState, record: DependencyState) -> WorkflowState:
    state.dependencies[
        dependency_key(record.upstream_instance_id, record.downstream_instance_id)
    ] = record
    return state


def set_submission(state: WorkflowState, record: DispatchSubmissionState) -> WorkflowState:
    state.submissions[record.submission_id] = record
    return state


def queue_instance(state: WorkflowState, stage_instance_id: str) -> WorkflowState:
    if stage_instance_id not in state.dispatch_queue:
        state.dispatch_queue = (*state.dispatch_queue, stage_instance_id)
    return state


def dequeue_instances(
    state: WorkflowState, stage_instance_ids: set[str] | tuple[str, ...]
) -> WorkflowState:
    remove = set(stage_instance_ids)
    state.dispatch_queue = tuple(
        stage_instance_id
        for stage_instance_id in state.dispatch_queue
        if stage_instance_id not in remove
    )
    return state
