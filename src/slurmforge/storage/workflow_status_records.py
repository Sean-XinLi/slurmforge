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

WORKFLOW_STATUS_STATES = (
    WORKFLOW_PLANNED,
    WORKFLOW_STREAMING,
    WORKFLOW_FINALIZING,
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILED,
    WORKFLOW_BLOCKED,
)

WORKFLOW_CONTROL_STATES = ("submitting", "submitted", "uncertain", "failed")


@dataclass
class WorkflowStatusControlJobRecord:
    key: str
    kind: str
    target_kind: str
    target_id: str
    state: str
    sbatch_paths: tuple[str, ...] = ()
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    dependency_job_ids: tuple[str, ...] = ()
    reason: str = ""
    started_at: str = ""
    submitted_at: str = ""
    failed_at: str = ""


@dataclass
class WorkflowStatusRecord:
    state: str
    updated_at: str
    reason: str = ""
    control_jobs: dict[str, WorkflowStatusControlJobRecord] = field(
        default_factory=dict
    )
    stage_jobs: dict[str, dict[str, str]] = field(default_factory=dict)
    instances: dict[str, dict[str, Any]] = field(default_factory=dict)
    dependencies: dict[str, dict[str, Any]] = field(default_factory=dict)
    dispatch_queue: tuple[str, ...] = ()
    submissions: dict[str, dict[str, Any]] = field(default_factory=dict)
    terminal_aggregation: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SchemaVersion.WORKFLOW_STATUS


def workflow_status_from_dict(payload: dict[str, Any]) -> WorkflowStatusRecord:
    version = require_schema(
        payload, name="workflow_status", version=SchemaVersion.WORKFLOW_STATUS
    )
    state = _required_string(payload, "state")
    if state not in WORKFLOW_STATUS_STATES:
        raise RecordContractError(f"Unsupported workflow status state: {state}")
    return WorkflowStatusRecord(
        schema_version=version,
        state=state,
        updated_at=_required_string(payload, "updated_at"),
        reason=str(payload.get("reason") or ""),
        control_jobs=_control_jobs_from_payload(payload.get("control_jobs")),
        stage_jobs=_stage_jobs_from_payload(payload.get("stage_jobs")),
        instances=_object_field(payload, "instances"),
        dependencies=_object_field(payload, "dependencies"),
        dispatch_queue=tuple(
            str(item) for item in _array_field(payload, "dispatch_queue")
        ),
        submissions=_object_field(payload, "submissions"),
        terminal_aggregation=_object_field(payload, "terminal_aggregation"),
    )


def workflow_status_to_dict(record: WorkflowStatusRecord) -> dict[str, Any]:
    _validate_workflow_status_record(record)
    return to_jsonable(record)


def workflow_status_control_job_from_dict(
    key: str, payload: dict[str, Any]
) -> WorkflowStatusControlJobRecord:
    payload_key = _required_string(payload, "key")
    if payload_key != key:
        raise RecordContractError(
            f"workflow status control job key `{payload_key}` does not match `{key}`"
        )
    state = _required_string(payload, "state")
    scheduler_job_ids = _tuple_field(payload, "scheduler_job_ids")
    reason = str(payload.get("reason") or "")
    record = WorkflowStatusControlJobRecord(
        key=key,
        kind=_required_string(payload, "kind"),
        target_kind=_required_string(payload, "target_kind"),
        target_id=_required_string(payload, "target_id"),
        state=state,
        sbatch_paths=_tuple_field(payload, "sbatch_paths"),
        scheduler_job_ids=scheduler_job_ids,
        barrier_job_ids=_tuple_field(payload, "barrier_job_ids"),
        dependency_job_ids=_tuple_field(payload, "dependency_job_ids"),
        reason=reason,
        started_at=str(payload.get("started_at") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
        failed_at=str(payload.get("failed_at") or ""),
    )
    _validate_control_job(record)
    return record


def _validate_workflow_status_record(record: WorkflowStatusRecord) -> None:
    if record.schema_version != SchemaVersion.WORKFLOW_STATUS:
        raise RecordContractError("workflow_status schema_version is invalid")
    if record.state not in WORKFLOW_STATUS_STATES:
        raise RecordContractError(
            f"Unsupported workflow status state: {record.state}"
        )
    if not record.updated_at:
        raise RecordContractError("workflow_status.updated_at is required")
    for key, control_job in record.control_jobs.items():
        if key != control_job.key:
            raise RecordContractError(
                f"workflow status control job key `{control_job.key}` does not match `{key}`"
            )
        _validate_control_job(control_job)


def _validate_control_job(record: WorkflowStatusControlJobRecord) -> None:
    if record.state not in WORKFLOW_CONTROL_STATES:
        raise RecordContractError(
            f"Unsupported workflow status control job state: {record.state}"
        )
    if not record.kind:
        raise RecordContractError("workflow status control job kind is required")
    if not record.target_kind:
        raise RecordContractError("workflow status control job target_kind is required")
    if not record.target_id:
        raise RecordContractError("workflow status control job target_id is required")
    if not record.sbatch_paths:
        raise RecordContractError("workflow status control job sbatch_paths is required")
    if record.state == "submitted" and not record.scheduler_job_ids:
        raise RecordContractError(
            "submitted workflow status control job requires scheduler_job_ids"
        )
    if record.state in {"failed", "uncertain"} and not record.reason:
        raise RecordContractError(
            f"{record.state} workflow status control job requires reason"
        )


def _control_jobs_from_payload(
    payload: Any,
) -> dict[str, WorkflowStatusControlJobRecord]:
    if not isinstance(payload, dict):
        raise RecordContractError("workflow_status.control_jobs must be an object")
    result: dict[str, WorkflowStatusControlJobRecord] = {}
    for key, value in payload.items():
        if not isinstance(value, dict):
            raise RecordContractError(
                f"workflow_status.control_jobs.{key} must be an object"
            )
        result[str(key)] = workflow_status_control_job_from_dict(str(key), dict(value))
    return result


def _stage_jobs_from_payload(payload: Any) -> dict[str, dict[str, str]]:
    if not isinstance(payload, dict):
        raise RecordContractError("workflow_status.stage_jobs must be an object")
    stage_jobs: dict[str, dict[str, str]] = {}
    for stage_key, groups in payload.items():
        if not isinstance(groups, dict):
            raise RecordContractError(
                f"workflow_status.stage_jobs.{stage_key} must be an object"
            )
        stage_jobs[str(stage_key)] = {
            str(group_id): str(job_id) for group_id, job_id in groups.items()
        }
    return stage_jobs


def _object_field(payload: dict[str, Any], field_name: str) -> dict[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, dict):
        raise RecordContractError(f"workflow_status.{field_name} must be an object")
    return dict(value)


def _array_field(payload: dict[str, Any], field_name: str) -> tuple[Any, ...]:
    value = payload.get(field_name)
    if not isinstance(value, (list, tuple)):
        raise RecordContractError(f"workflow_status.{field_name} must be an array")
    return tuple(value)


def _required_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"workflow_status.{field_name} must be a non-empty string"
        )
    return value


def _tuple_field(payload: dict[str, Any], field_name: str) -> tuple[str, ...]:
    value = payload.get(field_name)
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise RecordContractError(
            f"workflow_status control job `{field_name}` must be an array"
        )
    return tuple(str(item) for item in value)
