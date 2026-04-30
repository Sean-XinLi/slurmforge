from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..io import SchemaVersion, to_jsonable
from ..workflow_contract import WORKFLOW_PLANNED

INSTANCE_PLANNED = "planned"
INSTANCE_READY = "ready"
INSTANCE_SUBMITTED = "submitted"
INSTANCE_RUNNING = "running"
INSTANCE_SUCCESS = "success"
INSTANCE_FAILED = "failed"
INSTANCE_SKIPPED = "skipped"
INSTANCE_BLOCKED = "blocked"
INSTANCE_TERMINAL_STATES = (
    INSTANCE_SUCCESS,
    INSTANCE_FAILED,
    INSTANCE_SKIPPED,
    INSTANCE_BLOCKED,
)

DEPENDENCY_WAITING = "waiting"
DEPENDENCY_READY = "ready"
DEPENDENCY_RESOLVED = "resolved"
DEPENDENCY_BLOCKED = "blocked"

DISPATCH_SUBMITTING = "submitting"
DISPATCH_SUBMITTED = "submitted"
DISPATCH_TERMINAL = "terminal"
DISPATCH_FAILED = "failed"
DISPATCH_ACTIVE_STATES = (DISPATCH_SUBMITTING, DISPATCH_SUBMITTED)

RELEASE_PER_RUN = "per_run"
RELEASE_PER_GROUP = "per_group"
RELEASE_PER_STAGE = "per_stage"
RELEASE_WINDOWED = "windowed"
RELEASE_POLICIES = (
    RELEASE_PER_RUN,
    RELEASE_PER_GROUP,
    RELEASE_PER_STAGE,
    RELEASE_WINDOWED,
)


@dataclass
class StageInstanceState:
    stage_instance_id: str
    stage_name: str
    run_id: str
    state: str = INSTANCE_PLANNED
    submission_id: str = ""
    scheduler_job_id: str = ""
    scheduler_array_task_id: str = ""
    output_ready: bool = False
    reason: str = ""
    ready_at: str = ""


@dataclass
class DependencyState:
    upstream_instance_id: str
    downstream_instance_id: str
    condition: str = "success"
    state: str = DEPENDENCY_WAITING


@dataclass
class DispatchSubmissionState:
    submission_id: str
    stage_name: str
    instance_ids: tuple[str, ...]
    root: str
    scheduler_job_ids: tuple[str, ...] = ()
    budgeted_gpus: int = 0
    state: str = DISPATCH_SUBMITTING


@dataclass
class TerminalAggregationState:
    state: str = "pending"
    reason: str = ""
    notification_job_ids: tuple[str, ...] = ()
    completed_at: str = ""


@dataclass
class WorkflowState:
    pipeline_id: str
    pipeline_kind: str
    state: str = WORKFLOW_PLANNED
    current_stage: str = ""
    instances: dict[str, StageInstanceState] = field(default_factory=dict)
    dependencies: dict[str, DependencyState] = field(default_factory=dict)
    dispatch_queue: tuple[str, ...] = ()
    submissions: dict[str, DispatchSubmissionState] = field(default_factory=dict)
    terminal_aggregation: TerminalAggregationState = field(
        default_factory=TerminalAggregationState
    )
    release_policy: str = RELEASE_PER_RUN
    schema_version: int = SchemaVersion.WORKFLOW_STATE


def dependency_key(upstream_instance_id: str, downstream_instance_id: str) -> str:
    return f"{upstream_instance_id}->{downstream_instance_id}"


def workflow_state_from_dict(payload: dict[str, Any]) -> WorkflowState:
    terminal_aggregation = dict(payload.get("terminal_aggregation") or {})
    release_policy = str(payload.get("release_policy") or RELEASE_PER_RUN)
    if release_policy not in RELEASE_POLICIES:
        raise ValueError(f"unsupported release_policy: {release_policy}")
    return WorkflowState(
        pipeline_id=str(payload["pipeline_id"]),
        pipeline_kind=str(payload["pipeline_kind"]),
        state=str(payload.get("state") or WORKFLOW_PLANNED),
        current_stage=str(payload.get("current_stage") or ""),
        instances={
            str(instance_id): _instance_from_dict(dict(record))
            for instance_id, record in dict(payload.get("instances") or {}).items()
        },
        dependencies={
            str(dep_id): _dependency_from_dict(dict(record))
            for dep_id, record in dict(payload.get("dependencies") or {}).items()
        },
        dispatch_queue=tuple(
            str(instance_id) for instance_id in payload.get("dispatch_queue") or ()
        ),
        submissions={
            str(submission_id): _submission_from_dict(dict(record))
            for submission_id, record in dict(payload.get("submissions") or {}).items()
        },
        terminal_aggregation=TerminalAggregationState(
            state=str(terminal_aggregation.get("state") or "pending"),
            reason=str(terminal_aggregation.get("reason") or ""),
            notification_job_ids=tuple(
                str(item)
                for item in terminal_aggregation.get("notification_job_ids") or ()
            ),
            completed_at=str(terminal_aggregation.get("completed_at") or ""),
        ),
        release_policy=release_policy,
    )


def workflow_state_to_dict(state: WorkflowState) -> dict[str, Any]:
    return to_jsonable(state)


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


def _instance_from_dict(payload: dict[str, Any]) -> StageInstanceState:
    return StageInstanceState(
        stage_instance_id=str(payload["stage_instance_id"]),
        stage_name=str(payload["stage_name"]),
        run_id=str(payload["run_id"]),
        state=str(payload.get("state") or INSTANCE_PLANNED),
        submission_id=str(payload.get("submission_id") or ""),
        scheduler_job_id=str(payload.get("scheduler_job_id") or ""),
        scheduler_array_task_id=str(payload.get("scheduler_array_task_id") or ""),
        output_ready=bool(payload.get("output_ready") or False),
        reason=str(payload.get("reason") or ""),
        ready_at=str(payload.get("ready_at") or ""),
    )


def _dependency_from_dict(payload: dict[str, Any]) -> DependencyState:
    return DependencyState(
        upstream_instance_id=str(payload["upstream_instance_id"]),
        downstream_instance_id=str(payload["downstream_instance_id"]),
        condition=str(payload.get("condition") or "success"),
        state=str(payload.get("state") or DEPENDENCY_WAITING),
    )


def _submission_from_dict(payload: dict[str, Any]) -> DispatchSubmissionState:
    return DispatchSubmissionState(
        submission_id=str(payload["submission_id"]),
        stage_name=str(payload["stage_name"]),
        instance_ids=tuple(str(item) for item in payload.get("instance_ids") or ()),
        root=str(payload.get("root") or ""),
        scheduler_job_ids=tuple(
            str(item) for item in payload.get("scheduler_job_ids") or ()
        ),
        budgeted_gpus=int(payload.get("budgeted_gpus") or 0),
        state=str(payload.get("state") or DISPATCH_SUBMITTING),
    )


__all__ = [
    name
    for name in globals()
    if name.isupper()
    or name
    in {
        "StageInstanceState",
        "DependencyState",
        "DispatchSubmissionState",
        "TerminalAggregationState",
        "WorkflowState",
        "dependency_key",
        "workflow_state_from_dict",
        "workflow_state_to_dict",
        "set_instance",
        "set_dependency",
        "set_submission",
        "queue_instance",
        "dequeue_instances",
    }
]
