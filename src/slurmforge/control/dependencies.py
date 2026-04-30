from __future__ import annotations

from ..plans.train_eval import TrainEvalPipelinePlan
from ..io import utc_now
from ..workflow_contract import TRAIN_STAGE
from ..storage.workflow_state_records import (
    DEPENDENCY_BLOCKED,
    DEPENDENCY_READY,
    DEPENDENCY_RESOLVED,
    DEPENDENCY_WAITING,
    INSTANCE_BLOCKED,
    INSTANCE_PLANNED,
    INSTANCE_READY,
    INSTANCE_SUCCESS,
    INSTANCE_TERMINAL_STATES,
    RELEASE_PER_GROUP,
    RELEASE_PER_RUN,
    RELEASE_PER_STAGE,
    RELEASE_WINDOWED,
    WorkflowState,
    queue_instance,
)


def resolve_dependencies(
    plan: TrainEvalPipelinePlan, state: WorkflowState
) -> WorkflowState:
    policy = state.release_policy
    if policy in {RELEASE_PER_RUN, RELEASE_WINDOWED}:
        return _resolve_per_run(state)
    if policy == RELEASE_PER_GROUP:
        return _resolve_per_group(plan, state)
    if policy == RELEASE_PER_STAGE:
        return _resolve_per_stage(plan, state)
    raise ValueError(f"unsupported release_policy: {policy}")


def _resolve_per_run(state: WorkflowState) -> WorkflowState:
    for dependency in state.dependencies.values():
        upstream = state.instances.get(dependency.upstream_instance_id)
        downstream = state.instances.get(dependency.downstream_instance_id)
        if upstream is None or downstream is None:
            continue
        if upstream.state == INSTANCE_SUCCESS and upstream.output_ready:
            dependency.state = DEPENDENCY_RESOLVED
            _mark_ready(state, downstream.stage_instance_id)
        elif upstream.state in INSTANCE_TERMINAL_STATES:
            dependency.state = DEPENDENCY_BLOCKED
            _mark_blocked(state, downstream.stage_instance_id, upstream.reason)
        elif dependency.state == DEPENDENCY_WAITING:
            dependency.state = DEPENDENCY_WAITING
    return state


def _resolve_per_group(
    plan: TrainEvalPipelinePlan, state: WorkflowState
) -> WorkflowState:
    train_batch = plan.stage_batches[TRAIN_STAGE]
    for group in train_batch.group_plans:
        upstream_ids = tuple(group.stage_instance_ids)
        upstreams = [
            state.instances[instance_id]
            for instance_id in upstream_ids
            if instance_id in state.instances
        ]
        if not upstreams or not all(
            instance.state in INSTANCE_TERMINAL_STATES for instance in upstreams
        ):
            continue
        ready_upstreams = {
            instance.stage_instance_id
            for instance in upstreams
            if instance.state == INSTANCE_SUCCESS and instance.output_ready
        }
        _release_matching_dependencies(state, ready_upstreams)
        blocked_upstreams = set(upstream_ids) - ready_upstreams
        _block_matching_dependencies(state, blocked_upstreams)
    return state


def _resolve_per_stage(
    plan: TrainEvalPipelinePlan, state: WorkflowState
) -> WorkflowState:
    train_instance_ids = tuple(
        instance.stage_instance_id
        for instance in plan.stage_batches[TRAIN_STAGE].stage_instances
    )
    train_records = [
        state.instances[instance_id]
        for instance_id in train_instance_ids
        if instance_id in state.instances
    ]
    if not train_records or not all(
        instance.state in INSTANCE_TERMINAL_STATES for instance in train_records
    ):
        return state
    ready_upstreams = {
        instance.stage_instance_id
        for instance in train_records
        if instance.state == INSTANCE_SUCCESS and instance.output_ready
    }
    _release_matching_dependencies(state, ready_upstreams)
    _block_matching_dependencies(state, set(train_instance_ids) - ready_upstreams)
    return state


def _release_matching_dependencies(
    state: WorkflowState, upstream_instance_ids: set[str]
) -> None:
    for dependency in state.dependencies.values():
        if dependency.upstream_instance_id not in upstream_instance_ids:
            continue
        dependency.state = DEPENDENCY_READY
        _mark_ready(state, dependency.downstream_instance_id)


def _block_matching_dependencies(
    state: WorkflowState, upstream_instance_ids: set[str]
) -> None:
    for dependency in state.dependencies.values():
        if dependency.upstream_instance_id not in upstream_instance_ids:
            continue
        dependency.state = DEPENDENCY_BLOCKED
        upstream = state.instances.get(dependency.upstream_instance_id)
        _mark_blocked(
            state,
            dependency.downstream_instance_id,
            "" if upstream is None else upstream.reason,
        )


def _mark_ready(state: WorkflowState, stage_instance_id: str) -> None:
    downstream = state.instances.get(stage_instance_id)
    if downstream is None or downstream.state != INSTANCE_PLANNED:
        return
    downstream.state = INSTANCE_READY
    downstream.reason = ""
    downstream.ready_at = utc_now()
    queue_instance(state, stage_instance_id)


def _mark_blocked(state: WorkflowState, stage_instance_id: str, reason: str) -> None:
    downstream = state.instances.get(stage_instance_id)
    if downstream is None or downstream.state != INSTANCE_PLANNED:
        return
    downstream.state = INSTANCE_BLOCKED
    downstream.reason = reason or "upstream dependency failed"
