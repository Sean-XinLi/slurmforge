from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema, to_jsonable
from ..workflow_contract import (
    WORKFLOW_BLOCKED,
    WORKFLOW_FAILED,
    WORKFLOW_FINALIZING,
    WORKFLOW_PLANNED,
    WORKFLOW_STREAMING,
    WORKFLOW_SUCCESS,
)

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
INSTANCE_STATES = (
    INSTANCE_PLANNED,
    INSTANCE_READY,
    INSTANCE_SUBMITTED,
    INSTANCE_RUNNING,
    INSTANCE_SUCCESS,
    INSTANCE_FAILED,
    INSTANCE_SKIPPED,
    INSTANCE_BLOCKED,
)

DEPENDENCY_WAITING = "waiting"
DEPENDENCY_READY = "ready"
DEPENDENCY_RESOLVED = "resolved"
DEPENDENCY_BLOCKED = "blocked"
DEPENDENCY_STATES = (
    DEPENDENCY_WAITING,
    DEPENDENCY_READY,
    DEPENDENCY_RESOLVED,
    DEPENDENCY_BLOCKED,
)

DISPATCH_SUBMITTING = "submitting"
DISPATCH_SUBMITTED = "submitted"
DISPATCH_TERMINAL = "terminal"
DISPATCH_FAILED = "failed"
DISPATCH_ACTIVE_STATES = (DISPATCH_SUBMITTING, DISPATCH_SUBMITTED)
DISPATCH_STATES = (
    DISPATCH_SUBMITTING,
    DISPATCH_SUBMITTED,
    DISPATCH_TERMINAL,
    DISPATCH_FAILED,
)
DISPATCH_ROLE_INITIAL = "initial"
DISPATCH_ROLE_DISPATCH = "dispatch"
DISPATCH_ROLES = (DISPATCH_ROLE_INITIAL, DISPATCH_ROLE_DISPATCH)

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
WORKFLOW_STATES = (
    WORKFLOW_PLANNED,
    WORKFLOW_STREAMING,
    WORKFLOW_FINALIZING,
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILED,
    WORKFLOW_BLOCKED,
)
TERMINAL_AGGREGATION_PENDING = "pending"
TERMINAL_AGGREGATION_SUBMITTED = "submitted"
TERMINAL_AGGREGATION_FAILED = "failed"
TERMINAL_AGGREGATION_UNCERTAIN = "uncertain"
TERMINAL_AGGREGATION_DISABLED = "disabled"
TERMINAL_AGGREGATION_STATES = (
    TERMINAL_AGGREGATION_PENDING,
    TERMINAL_AGGREGATION_SUBMITTED,
    TERMINAL_AGGREGATION_FAILED,
    TERMINAL_AGGREGATION_UNCERTAIN,
    TERMINAL_AGGREGATION_DISABLED,
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
class DispatchGroupSubmissionState:
    group_id: str
    stage_instance_ids: tuple[str, ...]
    scheduler_job_id: str = ""
    array_size: int = 1
    array_throttle: int = 0
    gpus_per_task: int = 0
    task_ids_by_instance: dict[str, str] = field(default_factory=dict)
    stage_instance_gate_job_id: str = ""
    state: str = DISPATCH_SUBMITTED


@dataclass
class DispatchSubmissionState:
    submission_id: str
    stage_name: str
    role: str
    display_key: str
    instance_ids: tuple[str, ...]
    root: str
    groups: dict[str, DispatchGroupSubmissionState] = field(default_factory=dict)
    budgeted_gpus: int = 0
    state: str = DISPATCH_SUBMITTING


@dataclass
class TerminalAggregationState:
    state: str = "pending"
    workflow_terminal_state: str = ""
    reason: str = ""
    notification_control_key: str = ""
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
    require_schema(payload, name="workflow_state", version=SchemaVersion.WORKFLOW_STATE)
    terminal_aggregation = _required_object(payload, "terminal_aggregation")
    release_policy = _required_string(payload, "release_policy")
    if release_policy not in RELEASE_POLICIES:
        raise RecordContractError(f"unsupported release_policy: {release_policy}")
    state = WorkflowState(
        pipeline_id=_required_string(payload, "pipeline_id", non_empty=True),
        pipeline_kind=_required_string(payload, "pipeline_kind", non_empty=True),
        state=_required_string(payload, "state", non_empty=True),
        current_stage=_required_string(payload, "current_stage"),
        instances={
            instance_id: _instance_from_dict(_required_record(record, "instance"))
            for instance_id, record in _required_object(payload, "instances").items()
        },
        dependencies={
            dep_id: _dependency_from_dict(_required_record(record, "dependency"))
            for dep_id, record in _required_object(payload, "dependencies").items()
        },
        dispatch_queue=tuple(str(item) for item in _required_array(payload, "dispatch_queue")),
        submissions={
            submission_id: _submission_from_dict(_required_record(record, "submission"))
            for submission_id, record in _required_object(payload, "submissions").items()
        },
        terminal_aggregation=TerminalAggregationState(
            state=_required_string(terminal_aggregation, "state", non_empty=True),
            workflow_terminal_state=_required_string(
                terminal_aggregation, "workflow_terminal_state"
            ),
            reason=_required_string(terminal_aggregation, "reason"),
            notification_control_key=_required_string(
                terminal_aggregation, "notification_control_key"
            ),
            completed_at=_required_string(terminal_aggregation, "completed_at"),
        ),
        release_policy=release_policy,
    )
    _validate_workflow_state(state)
    return state


def workflow_state_to_dict(state: WorkflowState) -> dict[str, Any]:
    _validate_workflow_state(state)
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
        stage_instance_id=_required_string(payload, "stage_instance_id", non_empty=True),
        stage_name=_required_string(payload, "stage_name", non_empty=True),
        run_id=_required_string(payload, "run_id", non_empty=True),
        state=_required_string(payload, "state", non_empty=True),
        submission_id=_required_string(payload, "submission_id"),
        scheduler_job_id=_required_string(payload, "scheduler_job_id"),
        scheduler_array_task_id=_required_string(payload, "scheduler_array_task_id"),
        output_ready=_required_bool(payload, "output_ready"),
        reason=_required_string(payload, "reason"),
        ready_at=_required_string(payload, "ready_at"),
    )


def _dependency_from_dict(payload: dict[str, Any]) -> DependencyState:
    return DependencyState(
        upstream_instance_id=_required_string(
            payload, "upstream_instance_id", non_empty=True
        ),
        downstream_instance_id=_required_string(
            payload, "downstream_instance_id", non_empty=True
        ),
        condition=_required_string(payload, "condition", non_empty=True),
        state=_required_string(payload, "state", non_empty=True),
    )


def _dispatch_group_from_dict(payload: dict[str, Any]) -> DispatchGroupSubmissionState:
    return DispatchGroupSubmissionState(
        group_id=_required_string(payload, "group_id", non_empty=True),
        stage_instance_ids=tuple(
            str(item) for item in _required_array(payload, "stage_instance_ids")
        ),
        scheduler_job_id=_required_string(payload, "scheduler_job_id"),
        array_size=_required_int(payload, "array_size"),
        array_throttle=_required_int(payload, "array_throttle"),
        gpus_per_task=_required_int(payload, "gpus_per_task"),
        task_ids_by_instance={
            str(instance_id): str(task_id)
            for instance_id, task_id in _required_object(
                payload, "task_ids_by_instance"
            ).items()
        },
        stage_instance_gate_job_id=_required_string(
            payload, "stage_instance_gate_job_id"
        ),
        state=_required_string(payload, "state", non_empty=True),
    )


def _submission_from_dict(payload: dict[str, Any]) -> DispatchSubmissionState:
    return DispatchSubmissionState(
        submission_id=_required_string(payload, "submission_id", non_empty=True),
        stage_name=_required_string(payload, "stage_name", non_empty=True),
        role=_required_string(payload, "role", non_empty=True),
        display_key=_required_string(payload, "display_key", non_empty=True),
        instance_ids=tuple(str(item) for item in _required_array(payload, "instance_ids")),
        root=_required_string(payload, "root", non_empty=True),
        groups={
            group_id: _dispatch_group_from_dict(_required_record(record, "dispatch_group"))
            for group_id, record in _required_object(payload, "groups").items()
        },
        budgeted_gpus=_required_int(payload, "budgeted_gpus"),
        state=_required_string(payload, "state", non_empty=True),
    )


def _validate_workflow_state(state: WorkflowState) -> None:
    if state.schema_version != SchemaVersion.WORKFLOW_STATE:
        raise RecordContractError("workflow_state.schema_version is invalid")
    if state.state not in WORKFLOW_STATES:
        raise RecordContractError(f"unsupported workflow state: {state.state}")
    if state.release_policy not in RELEASE_POLICIES:
        raise RecordContractError(
            f"unsupported release_policy: {state.release_policy}"
        )
    if state.terminal_aggregation.state not in TERMINAL_AGGREGATION_STATES:
        raise RecordContractError(
            f"unsupported terminal aggregation state: {state.terminal_aggregation.state}"
        )
    for instance_id, instance in state.instances.items():
        _validate_instance(state, instance_id, instance)
    for dep_id, dependency in state.dependencies.items():
        _validate_dependency(state, dep_id, dependency)
    for instance_id in state.dispatch_queue:
        if instance_id not in state.instances:
            raise RecordContractError(
                f"dispatch_queue references unknown stage instance `{instance_id}`"
            )
        if state.instances[instance_id].state != INSTANCE_READY:
            raise RecordContractError(
                f"dispatch_queue instance `{instance_id}` must be ready"
            )
    for submission_id, submission in state.submissions.items():
        _validate_submission(state, submission_id, submission)
    _validate_terminal_invariants(state)


def _validate_instance(
    state: WorkflowState, instance_id: str, instance: StageInstanceState
) -> None:
    if instance_id != instance.stage_instance_id:
        raise RecordContractError(
            f"instance key `{instance_id}` does not match `{instance.stage_instance_id}`"
        )
    if instance.state not in INSTANCE_STATES:
        raise RecordContractError(
            f"unsupported stage instance state: {instance.state}"
        )
    if instance.submission_id and instance.submission_id not in state.submissions:
        raise RecordContractError(
            f"stage instance `{instance_id}` references unknown submission `{instance.submission_id}`"
        )
    if instance.state == INSTANCE_SUBMITTED and not instance.scheduler_job_id:
        raise RecordContractError(
            f"submitted stage instance `{instance_id}` requires scheduler_job_id"
        )
    if (
        instance.submission_id
        and instance.state in {INSTANCE_RUNNING, INSTANCE_SUCCESS, INSTANCE_FAILED}
        and not instance.scheduler_job_id
    ):
        raise RecordContractError(
            f"{instance.state} stage instance `{instance_id}` requires scheduler_job_id"
        )


def _validate_dependency(
    state: WorkflowState, dep_id: str, dependency: DependencyState
) -> None:
    expected = dependency_key(
        dependency.upstream_instance_id, dependency.downstream_instance_id
    )
    if dep_id != expected:
        raise RecordContractError(
            f"dependency key `{dep_id}` does not match `{expected}`"
        )
    if dependency.state not in DEPENDENCY_STATES:
        raise RecordContractError(f"unsupported dependency state: {dependency.state}")
    if dependency.upstream_instance_id not in state.instances:
        raise RecordContractError(
            f"dependency `{dep_id}` references unknown upstream instance"
        )
    if dependency.downstream_instance_id not in state.instances:
        raise RecordContractError(
            f"dependency `{dep_id}` references unknown downstream instance"
        )


def _validate_submission(
    state: WorkflowState, submission_id: str, submission: DispatchSubmissionState
) -> None:
    if submission_id != submission.submission_id:
        raise RecordContractError(
            f"submission key `{submission_id}` does not match `{submission.submission_id}`"
        )
    if submission.state not in DISPATCH_STATES:
        raise RecordContractError(f"unsupported dispatch submission state: {submission.state}")
    if submission.role not in DISPATCH_ROLES:
        raise RecordContractError(f"unsupported dispatch submission role: {submission.role}")
    instance_ids = set(submission.instance_ids)
    for instance_id in submission.instance_ids:
        if instance_id not in state.instances:
            raise RecordContractError(
                f"submission `{submission_id}` references unknown stage instance `{instance_id}`"
            )
    for group_id, group in submission.groups.items():
        _validate_dispatch_group(submission_id, group_id, group, instance_ids)


def _validate_dispatch_group(
    submission_id: str,
    group_id: str,
    group: DispatchGroupSubmissionState,
    submission_instance_ids: set[str],
) -> None:
    if group_id != group.group_id:
        raise RecordContractError(
            f"dispatch group key `{group_id}` does not match `{group.group_id}`"
        )
    if group.state not in DISPATCH_STATES:
        raise RecordContractError(f"unsupported dispatch group state: {group.state}")
    if group.array_size < 1:
        raise RecordContractError(f"dispatch group `{group_id}` array_size must be >= 1")
    if group.array_throttle < 0:
        raise RecordContractError(
            f"dispatch group `{group_id}` array_throttle must be >= 0"
        )
    for instance_id in group.stage_instance_ids:
        if instance_id not in submission_instance_ids:
            raise RecordContractError(
                f"dispatch group `{group_id}` in `{submission_id}` references instance outside submission"
            )
    extra_task_ids = set(group.task_ids_by_instance) - set(group.stage_instance_ids)
    if extra_task_ids:
        raise RecordContractError(
            f"dispatch group `{group_id}` has task ids for unknown instances: {sorted(extra_task_ids)}"
        )
    if group.array_size > 1:
        missing = set(group.stage_instance_ids) - set(group.task_ids_by_instance)
        if missing:
            raise RecordContractError(
                f"array dispatch group `{group_id}` missing task ids for {sorted(missing)}"
            )


def _validate_terminal_invariants(state: WorkflowState) -> None:
    terminal_aggregation = state.terminal_aggregation
    has_terminal_aggregation = bool(
        terminal_aggregation.completed_at
        or terminal_aggregation.workflow_terminal_state
    )
    if state.state in {WORKFLOW_SUCCESS, WORKFLOW_BLOCKED} or has_terminal_aggregation:
        if not all(
            instance.state in INSTANCE_TERMINAL_STATES
            for instance in state.instances.values()
        ):
            raise RecordContractError(
                "terminal workflow aggregation requires all stage instances terminal"
            )
    if state.state not in {WORKFLOW_SUCCESS, WORKFLOW_FAILED, WORKFLOW_BLOCKED}:
        if terminal_aggregation.completed_at:
            raise RecordContractError(
                "non-terminal workflow state cannot have terminal completed_at"
            )


def _required_record(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecordContractError(f"workflow_state {label} must be an object")
    return dict(value)


def _required_string(
    payload: dict[str, Any],
    field_name: str,
    *,
    non_empty: bool = False,
) -> str:
    if field_name not in payload:
        raise RecordContractError(f"workflow_state.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, str):
        raise RecordContractError(f"workflow_state.{field_name} must be a string")
    if non_empty and not value:
        raise RecordContractError(
            f"workflow_state.{field_name} must be a non-empty string"
        )
    return value


def _required_bool(payload: dict[str, Any], field_name: str) -> bool:
    if field_name not in payload:
        raise RecordContractError(f"workflow_state.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, bool):
        raise RecordContractError(f"workflow_state.{field_name} must be a bool")
    return value


def _required_int(payload: dict[str, Any], field_name: str) -> int:
    if field_name not in payload:
        raise RecordContractError(f"workflow_state.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecordContractError(f"workflow_state.{field_name} must be an integer")
    return value


def _required_object(payload: dict[str, Any], field_name: str) -> dict[str, Any]:
    if field_name not in payload:
        raise RecordContractError(f"workflow_state.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, dict):
        raise RecordContractError(f"workflow_state.{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _required_array(payload: dict[str, Any], field_name: str) -> tuple[Any, ...]:
    if field_name not in payload:
        raise RecordContractError(f"workflow_state.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, (list, tuple)):
        raise RecordContractError(f"workflow_state.{field_name} must be an array")
    return tuple(value)


__all__ = [
    name
    for name in globals()
    if name.isupper()
    or name
    in {
        "StageInstanceState",
        "DependencyState",
        "DispatchGroupSubmissionState",
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
