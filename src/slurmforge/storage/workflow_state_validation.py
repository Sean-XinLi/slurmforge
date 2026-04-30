from __future__ import annotations

from ..errors import RecordContractError
from ..io import SchemaVersion
from ..workflow_contract import WORKFLOW_BLOCKED, WORKFLOW_FAILED, WORKFLOW_SUCCESS
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
    RELEASE_POLICIES,
    TERMINAL_AGGREGATION_STATES,
    WORKFLOW_STATES,
)
from .workflow_state_models import (
    DependencyState,
    DispatchGroupSubmissionState,
    DispatchSubmissionState,
    StageInstanceState,
    WorkflowState,
    dependency_key,
)


def validate_workflow_state(state: WorkflowState) -> None:
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
