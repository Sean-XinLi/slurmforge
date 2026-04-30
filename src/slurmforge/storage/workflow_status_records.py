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
from ..workflow_enums import CONTROL_STATE_SUBMITTED, CONTROL_STATES
from ..record_fields import (
    required_object,
    required_record,
    required_string,
    required_string_tuple,
)

WORKFLOW_STATUS_STATES = (
    WORKFLOW_PLANNED,
    WORKFLOW_STREAMING,
    WORKFLOW_FINALIZING,
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILED,
    WORKFLOW_BLOCKED,
)


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
    schema_version: int = SchemaVersion.WORKFLOW_STATUS


def workflow_status_from_dict(payload: dict[str, Any]) -> WorkflowStatusRecord:
    version = require_schema(
        payload, name="workflow_status", version=SchemaVersion.WORKFLOW_STATUS
    )
    state = required_string(
        payload, "state", label="workflow_status", non_empty=True
    )
    if state not in WORKFLOW_STATUS_STATES:
        raise RecordContractError(f"Unsupported workflow status state: {state}")
    record = WorkflowStatusRecord(
        schema_version=version,
        state=state,
        updated_at=required_string(
            payload, "updated_at", label="workflow_status", non_empty=True
        ),
        reason=required_string(payload, "reason", label="workflow_status"),
        control_jobs=_control_jobs_from_payload(
            required_object(payload, "control_jobs", label="workflow_status")
        ),
        stage_jobs=_stage_jobs_from_payload(
            required_object(payload, "stage_jobs", label="workflow_status")
        ),
    )
    _validate_workflow_status_record(record)
    return record


def workflow_status_to_dict(record: WorkflowStatusRecord) -> dict[str, Any]:
    _validate_workflow_status_record(record)
    return to_jsonable(record)


def workflow_status_control_job_from_dict(
    key: str, payload: dict[str, Any]
) -> WorkflowStatusControlJobRecord:
    payload_key = required_string(
        payload, "key", label="workflow_status control job", non_empty=True
    )
    if payload_key != key:
        raise RecordContractError(
            f"workflow status control job key `{payload_key}` does not match `{key}`"
        )
    record = WorkflowStatusControlJobRecord(
        key=key,
        kind=required_string(
            payload, "kind", label="workflow_status control job", non_empty=True
        ),
        target_kind=required_string(
            payload, "target_kind", label="workflow_status control job", non_empty=True
        ),
        target_id=required_string(
            payload, "target_id", label="workflow_status control job", non_empty=True
        ),
        state=required_string(
            payload, "state", label="workflow_status control job", non_empty=True
        ),
        sbatch_paths=required_string_tuple(
            payload, "sbatch_paths", label="workflow_status control job"
        ),
        scheduler_job_ids=required_string_tuple(
            payload, "scheduler_job_ids", label="workflow_status control job"
        ),
        barrier_job_ids=required_string_tuple(
            payload, "barrier_job_ids", label="workflow_status control job"
        ),
        dependency_job_ids=required_string_tuple(
            payload, "dependency_job_ids", label="workflow_status control job"
        ),
        reason=required_string(payload, "reason", label="workflow_status control job"),
        started_at=required_string(
            payload, "started_at", label="workflow_status control job"
        ),
        submitted_at=required_string(
            payload, "submitted_at", label="workflow_status control job"
        ),
        failed_at=required_string(
            payload, "failed_at", label="workflow_status control job"
        ),
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
    if record.state not in CONTROL_STATES:
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
    if record.state == CONTROL_STATE_SUBMITTED and not record.scheduler_job_ids:
        raise RecordContractError(
            "submitted workflow status control job requires scheduler_job_ids"
        )
    if record.state in {"failed", "uncertain"} and not record.reason:
        raise RecordContractError(
            f"{record.state} workflow status control job requires reason"
        )


def _control_jobs_from_payload(
    payload: dict[str, Any],
) -> dict[str, WorkflowStatusControlJobRecord]:
    result: dict[str, WorkflowStatusControlJobRecord] = {}
    for key, value in payload.items():
        result[str(key)] = workflow_status_control_job_from_dict(
            str(key), required_record(value, f"workflow_status.control_jobs.{key}")
        )
    return result


def _stage_jobs_from_payload(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for stage_key, groups in payload.items():
        group_payload = required_record(groups, f"workflow_status.stage_jobs.{stage_key}")
        stage_jobs[str(stage_key)] = {
            str(group_id): _job_id(job_id, stage_key=str(stage_key), group_id=str(group_id))
            for group_id, job_id in group_payload.items()
        }
    return stage_jobs


def _job_id(value: Any, *, stage_key: str, group_id: str) -> str:
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"workflow_status.stage_jobs.{stage_key}.{group_id} must be a non-empty string"
        )
    return value
