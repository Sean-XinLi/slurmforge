from __future__ import annotations

from typing import Any

from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema, to_jsonable
from ..record_fields import (
    required_array,
    required_bool,
    required_int,
    required_object,
    required_record,
    required_string,
)
from .workflow_state_constants import RELEASE_POLICIES
from .workflow_state_models import (
    DependencyState,
    DispatchGroupSubmissionState,
    DispatchSubmissionState,
    StageInstanceState,
    TerminalAggregationState,
    WorkflowState,
)
from .workflow_state_validation import validate_workflow_state

WORKFLOW_STATE_LABEL = "workflow_state"


def workflow_state_from_dict(payload: dict[str, Any]) -> WorkflowState:
    require_schema(payload, name=WORKFLOW_STATE_LABEL, version=SchemaVersion.WORKFLOW_STATE)
    terminal_aggregation = required_object(
        payload, "terminal_aggregation", label=WORKFLOW_STATE_LABEL
    )
    release_policy = required_string(payload, "release_policy", label=WORKFLOW_STATE_LABEL)
    if release_policy not in RELEASE_POLICIES:
        raise RecordContractError(f"unsupported release_policy: {release_policy}")
    state = WorkflowState(
        pipeline_id=required_string(
            payload, "pipeline_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        pipeline_kind=required_string(
            payload, "pipeline_kind", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        state=required_string(
            payload, "state", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        current_stage=required_string(payload, "current_stage", label=WORKFLOW_STATE_LABEL),
        instances={
            instance_id: _instance_from_dict(required_record(record, "workflow_state instance"))
            for instance_id, record in required_object(
                payload, "instances", label=WORKFLOW_STATE_LABEL
            ).items()
        },
        dependencies={
            dep_id: _dependency_from_dict(required_record(record, "workflow_state dependency"))
            for dep_id, record in required_object(
                payload, "dependencies", label=WORKFLOW_STATE_LABEL
            ).items()
        },
        dispatch_queue=tuple(
            str(item)
            for item in required_array(payload, "dispatch_queue", label=WORKFLOW_STATE_LABEL)
        ),
        submissions={
            submission_id: _submission_from_dict(
                required_record(record, "workflow_state submission")
            )
            for submission_id, record in required_object(
                payload, "submissions", label=WORKFLOW_STATE_LABEL
            ).items()
        },
        terminal_aggregation=TerminalAggregationState(
            state=required_string(
                terminal_aggregation,
                "state",
                label=WORKFLOW_STATE_LABEL,
                non_empty=True,
            ),
            workflow_terminal_state=required_string(
                terminal_aggregation,
                "workflow_terminal_state",
                label=WORKFLOW_STATE_LABEL,
            ),
            reason=required_string(
                terminal_aggregation, "reason", label=WORKFLOW_STATE_LABEL
            ),
            notification_control_key=required_string(
                terminal_aggregation,
                "notification_control_key",
                label=WORKFLOW_STATE_LABEL,
            ),
            completed_at=required_string(
                terminal_aggregation, "completed_at", label=WORKFLOW_STATE_LABEL
            ),
        ),
        release_policy=release_policy,
    )
    validate_workflow_state(state)
    return state


def workflow_state_to_dict(state: WorkflowState) -> dict[str, Any]:
    validate_workflow_state(state)
    return to_jsonable(state)


def _instance_from_dict(payload: dict[str, Any]) -> StageInstanceState:
    return StageInstanceState(
        stage_instance_id=required_string(
            payload, "stage_instance_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        run_id=required_string(
            payload, "run_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        state=required_string(
            payload, "state", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        submission_id=required_string(
            payload, "submission_id", label=WORKFLOW_STATE_LABEL
        ),
        scheduler_job_id=required_string(
            payload, "scheduler_job_id", label=WORKFLOW_STATE_LABEL
        ),
        scheduler_array_task_id=required_string(
            payload, "scheduler_array_task_id", label=WORKFLOW_STATE_LABEL
        ),
        output_ready=required_bool(payload, "output_ready", label=WORKFLOW_STATE_LABEL),
        reason=required_string(payload, "reason", label=WORKFLOW_STATE_LABEL),
        ready_at=required_string(payload, "ready_at", label=WORKFLOW_STATE_LABEL),
    )


def _dependency_from_dict(payload: dict[str, Any]) -> DependencyState:
    return DependencyState(
        upstream_instance_id=required_string(
            payload, "upstream_instance_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        downstream_instance_id=required_string(
            payload, "downstream_instance_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        condition=required_string(
            payload, "condition", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        state=required_string(
            payload, "state", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
    )


def _dispatch_group_from_dict(payload: dict[str, Any]) -> DispatchGroupSubmissionState:
    return DispatchGroupSubmissionState(
        group_id=required_string(
            payload, "group_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        stage_instance_ids=tuple(
            str(item)
            for item in required_array(
                payload, "stage_instance_ids", label=WORKFLOW_STATE_LABEL
            )
        ),
        scheduler_job_id=required_string(
            payload, "scheduler_job_id", label=WORKFLOW_STATE_LABEL
        ),
        array_size=required_int(payload, "array_size", label=WORKFLOW_STATE_LABEL),
        array_throttle=required_int(
            payload, "array_throttle", label=WORKFLOW_STATE_LABEL
        ),
        gpus_per_task=required_int(
            payload, "gpus_per_task", label=WORKFLOW_STATE_LABEL
        ),
        task_ids_by_instance={
            str(instance_id): str(task_id)
            for instance_id, task_id in required_object(
                payload, "task_ids_by_instance", label=WORKFLOW_STATE_LABEL
            ).items()
        },
        stage_instance_gate_job_id=required_string(
            payload, "stage_instance_gate_job_id", label=WORKFLOW_STATE_LABEL
        ),
        state=required_string(
            payload, "state", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
    )


def _submission_from_dict(payload: dict[str, Any]) -> DispatchSubmissionState:
    return DispatchSubmissionState(
        submission_id=required_string(
            payload, "submission_id", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        role=required_string(
            payload, "role", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        display_key=required_string(
            payload, "display_key", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
        instance_ids=tuple(
            str(item)
            for item in required_array(payload, "instance_ids", label=WORKFLOW_STATE_LABEL)
        ),
        root=required_string(payload, "root", label=WORKFLOW_STATE_LABEL, non_empty=True),
        groups={
            group_id: _dispatch_group_from_dict(
                required_record(record, "workflow_state dispatch_group")
            )
            for group_id, record in required_object(
                payload, "groups", label=WORKFLOW_STATE_LABEL
            ).items()
        },
        budgeted_gpus=required_int(
            payload, "budgeted_gpus", label=WORKFLOW_STATE_LABEL
        ),
        state=required_string(
            payload, "state", label=WORKFLOW_STATE_LABEL, non_empty=True
        ),
    )
