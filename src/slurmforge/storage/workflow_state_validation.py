from __future__ import annotations

from ..errors import RecordContractError
from ..io import SchemaVersion
from ..release_policy_contract import RELEASE_POLICIES
from ..workflow_contract import (
    WORKFLOW_BLOCKED,
    WORKFLOW_FAILED,
    WORKFLOW_STATES,
    WORKFLOW_SUCCESS,
)
from .workflow_state_constants import (
    DEPENDENCY_STATES,
    DISPATCH_ROLES,
    DISPATCH_STATES,
    INSTANCE_FAILED,
    INSTANCE_READY,
    INSTANCE_RUNNING,
    INSTANCE_STATES,
    INSTANCE_SUBMITTED,
    INSTANCE_SUCCESS,
    INSTANCE_TERMINAL_STATES,
    TERMINAL_AGGREGATION_STATES,
)
from .workflow_state_models import (
    DependencyState,
    DispatchGroupSubmissionState,
    DispatchSubmissionState,
    StageInstanceState,
    TerminalAggregationState,
    WorkflowState,
    dependency_key,
)


def validate_workflow_state(state: WorkflowState) -> None:
    _int_field(state.schema_version, label="workflow_state.schema_version")
    if state.schema_version != SchemaVersion.WORKFLOW_STATE:
        raise RecordContractError("workflow_state.schema_version is invalid")
    _string_field(state.pipeline_id, label="workflow_state.pipeline_id", non_empty=True)
    _string_field(
        state.pipeline_kind, label="workflow_state.pipeline_kind", non_empty=True
    )
    _string_field(state.state, label="workflow_state.state", non_empty=True)
    if state.state not in WORKFLOW_STATES:
        raise RecordContractError(f"unsupported workflow state: {state.state}")
    _string_field(state.current_stage, label="workflow_state.current_stage")
    _string_field(
        state.release_policy, label="workflow_state.release_policy", non_empty=True
    )
    if state.release_policy not in RELEASE_POLICIES:
        raise RecordContractError(
            f"unsupported release_policy: {state.release_policy}"
        )
    _validate_terminal_aggregation(state.terminal_aggregation)
    if state.terminal_aggregation.state not in TERMINAL_AGGREGATION_STATES:
        raise RecordContractError(
            f"unsupported terminal aggregation state: {state.terminal_aggregation.state}"
        )
    _record_map_field(
        state.instances,
        label="workflow_state.instances",
        record_type=StageInstanceState,
    )
    _record_map_field(
        state.dependencies,
        label="workflow_state.dependencies",
        record_type=DependencyState,
    )
    _string_tuple_field(state.dispatch_queue, label="workflow_state.dispatch_queue")
    _record_map_field(
        state.submissions,
        label="workflow_state.submissions",
        record_type=DispatchSubmissionState,
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
    _string_field(instance_id, label="workflow_state.instances key", non_empty=True)
    _string_field(
        instance.stage_instance_id,
        label=f"workflow_state.instances.{instance_id}.stage_instance_id",
        non_empty=True,
    )
    _string_field(
        instance.stage_name,
        label=f"workflow_state.instances.{instance_id}.stage_name",
        non_empty=True,
    )
    _string_field(
        instance.run_id,
        label=f"workflow_state.instances.{instance_id}.run_id",
        non_empty=True,
    )
    _string_field(
        instance.state,
        label=f"workflow_state.instances.{instance_id}.state",
        non_empty=True,
    )
    _string_field(
        instance.submission_id,
        label=f"workflow_state.instances.{instance_id}.submission_id",
    )
    _string_field(
        instance.scheduler_job_id,
        label=f"workflow_state.instances.{instance_id}.scheduler_job_id",
    )
    _string_field(
        instance.scheduler_array_task_id,
        label=f"workflow_state.instances.{instance_id}.scheduler_array_task_id",
    )
    _bool_field(
        instance.output_ready,
        label=f"workflow_state.instances.{instance_id}.output_ready",
    )
    _string_field(instance.reason, label=f"workflow_state.instances.{instance_id}.reason")
    _string_field(
        instance.ready_at, label=f"workflow_state.instances.{instance_id}.ready_at"
    )
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
    _string_field(dep_id, label="workflow_state.dependencies key", non_empty=True)
    _string_field(
        dependency.upstream_instance_id,
        label=f"workflow_state.dependencies.{dep_id}.upstream_instance_id",
        non_empty=True,
    )
    _string_field(
        dependency.downstream_instance_id,
        label=f"workflow_state.dependencies.{dep_id}.downstream_instance_id",
        non_empty=True,
    )
    _string_field(
        dependency.condition,
        label=f"workflow_state.dependencies.{dep_id}.condition",
        non_empty=True,
    )
    _string_field(
        dependency.state,
        label=f"workflow_state.dependencies.{dep_id}.state",
        non_empty=True,
    )
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
    _string_field(submission_id, label="workflow_state.submissions key", non_empty=True)
    _string_field(
        submission.submission_id,
        label=f"workflow_state.submissions.{submission_id}.submission_id",
        non_empty=True,
    )
    _string_field(
        submission.stage_name,
        label=f"workflow_state.submissions.{submission_id}.stage_name",
        non_empty=True,
    )
    _string_field(
        submission.role,
        label=f"workflow_state.submissions.{submission_id}.role",
        non_empty=True,
    )
    _string_field(
        submission.display_key,
        label=f"workflow_state.submissions.{submission_id}.display_key",
        non_empty=True,
    )
    _string_tuple_field(
        submission.instance_ids,
        label=f"workflow_state.submissions.{submission_id}.instance_ids",
    )
    _string_field(
        submission.root,
        label=f"workflow_state.submissions.{submission_id}.root",
        non_empty=True,
    )
    _record_map_field(
        submission.groups,
        label=f"workflow_state.submissions.{submission_id}.groups",
        record_type=DispatchGroupSubmissionState,
    )
    _int_field(
        submission.budgeted_gpus,
        label=f"workflow_state.submissions.{submission_id}.budgeted_gpus",
    )
    _string_field(
        submission.state,
        label=f"workflow_state.submissions.{submission_id}.state",
        non_empty=True,
    )
    if submission_id != submission.submission_id:
        raise RecordContractError(
            f"submission key `{submission_id}` does not match `{submission.submission_id}`"
        )
    if submission.state not in DISPATCH_STATES:
        raise RecordContractError(
            f"unsupported dispatch submission state: {submission.state}"
        )
    if submission.role not in DISPATCH_ROLES:
        raise RecordContractError(
            f"unsupported dispatch submission role: {submission.role}"
        )
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
    _string_field(
        group_id,
        label=f"workflow_state.submissions.{submission_id}.groups key",
        non_empty=True,
    )
    _string_field(
        group.group_id,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.group_id",
        non_empty=True,
    )
    _string_tuple_field(
        group.stage_instance_ids,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.stage_instance_ids",
    )
    _string_field(
        group.scheduler_job_id,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.scheduler_job_id",
    )
    _int_field(
        group.array_size,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.array_size",
    )
    _int_field(
        group.array_throttle,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.array_throttle",
    )
    _int_field(
        group.gpus_per_task,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.gpus_per_task",
    )
    _string_map_field(
        group.task_ids_by_instance,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.task_ids_by_instance",
    )
    _string_field(
        group.stage_instance_gate_job_id,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.stage_instance_gate_job_id",
    )
    _string_field(
        group.state,
        label=f"workflow_state.submissions.{submission_id}.groups.{group_id}.state",
        non_empty=True,
    )
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


def _validate_terminal_aggregation(value) -> None:
    if not isinstance(value, TerminalAggregationState):
        raise RecordContractError(
            "workflow_state.terminal_aggregation must be a TerminalAggregationState"
        )
    _string_field(
        value.state, label="workflow_state.terminal_aggregation.state", non_empty=True
    )
    _string_field(
        value.workflow_terminal_state,
        label="workflow_state.terminal_aggregation.workflow_terminal_state",
    )
    _string_field(value.reason, label="workflow_state.terminal_aggregation.reason")
    _string_field(
        value.notification_control_key,
        label="workflow_state.terminal_aggregation.notification_control_key",
    )
    _string_field(
        value.completed_at, label="workflow_state.terminal_aggregation.completed_at"
    )


def _string_field(value, *, label: str, non_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise RecordContractError(f"{label} must be a string")
    if non_empty and not value:
        raise RecordContractError(f"{label} must be a non-empty string")
    return value


def _bool_field(value, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise RecordContractError(f"{label} must be a bool")
    return value


def _int_field(value, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecordContractError(f"{label} must be an integer")
    return value


def _record_map_field(value, *, label: str, record_type: type) -> dict:
    if not isinstance(value, dict):
        raise RecordContractError(f"{label} must be an object")
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise RecordContractError(f"{label} keys must be non-empty strings")
        if not isinstance(item, record_type):
            raise RecordContractError(f"{label}.{key} must be a {record_type.__name__}")
    return value


def _string_tuple_field(value, *, label: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise RecordContractError(f"{label} must be a tuple")
    for item in value:
        if not isinstance(item, str) or not item:
            raise RecordContractError(f"{label} items must be non-empty strings")
    return value


def _string_map_field(value, *, label: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise RecordContractError(f"{label} must be an object")
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise RecordContractError(f"{label} keys must be non-empty strings")
        if not isinstance(item, str):
            raise RecordContractError(f"{label}.{key} must be a string")
    return value
