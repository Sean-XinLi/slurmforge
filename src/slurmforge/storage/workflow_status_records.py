from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..control_job_contract import (
    ControlJobRecord,
    control_job_from_payload,
    validate_control_job_record,
)
from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema, to_jsonable
from ..record_fields import (
    required_object,
    required_record,
    required_string,
)
from ..workflow_contract import WORKFLOW_STATES


@dataclass
class WorkflowStatusRecord:
    state: str
    updated_at: str
    reason: str = ""
    control_jobs: dict[str, ControlJobRecord] = field(default_factory=dict)
    stage_jobs: dict[str, dict[str, str]] = field(default_factory=dict)
    schema_version: int = SchemaVersion.WORKFLOW_STATUS


def workflow_status_from_dict(payload: dict[str, Any]) -> WorkflowStatusRecord:
    version = require_schema(
        payload, name="workflow_status", version=SchemaVersion.WORKFLOW_STATUS
    )
    state = required_string(
        payload, "state", label="workflow_status", non_empty=True
    )
    if state not in WORKFLOW_STATES:
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
) -> ControlJobRecord:
    return control_job_from_payload(
        key,
        payload,
        label="workflow status control job",
    )


def _validate_workflow_status_record(record: WorkflowStatusRecord) -> None:
    if record.schema_version != SchemaVersion.WORKFLOW_STATUS:
        raise RecordContractError("workflow_status schema_version is invalid")
    if record.state not in WORKFLOW_STATES:
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
        validate_control_job_record(control_job, label="workflow status control job")
    _validate_stage_jobs(record.stage_jobs)


def _control_jobs_from_payload(
    payload: dict[str, Any],
) -> dict[str, ControlJobRecord]:
    result: dict[str, ControlJobRecord] = {}
    for key, value in payload.items():
        result[str(key)] = workflow_status_control_job_from_dict(
            str(key), required_record(value, f"workflow_status.control_jobs.{key}")
        )
    return result


def _stage_jobs_from_payload(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for stage_key, groups in payload.items():
        stage = _non_empty_string(stage_key, label="workflow_status.stage_jobs key")
        group_payload = required_record(groups, f"workflow_status.stage_jobs.{stage}")
        stage_jobs[stage] = {
            _non_empty_string(
                group_id, label=f"workflow_status.stage_jobs.{stage} group key"
            ): _job_id(job_id, stage_key=stage, group_id=str(group_id))
            for group_id, job_id in group_payload.items()
        }
    return stage_jobs


def _validate_stage_jobs(stage_jobs: Any) -> None:
    if not isinstance(stage_jobs, dict):
        raise RecordContractError("workflow_status.stage_jobs must be an object")
    for stage_key, groups in stage_jobs.items():
        stage = _non_empty_string(stage_key, label="workflow_status.stage_jobs key")
        if not isinstance(groups, dict):
            raise RecordContractError(f"workflow_status.stage_jobs.{stage} must be an object")
        for group_id, job_id in groups.items():
            group = _non_empty_string(
                group_id, label=f"workflow_status.stage_jobs.{stage} group key"
            )
            _job_id(job_id, stage_key=stage, group_id=group)


def _non_empty_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise RecordContractError(f"{label} must be a non-empty string")
    return value


def _job_id(value: Any, *, stage_key: str, group_id: str) -> str:
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"workflow_status.stage_jobs.{stage_key}.{group_id} must be a non-empty string"
        )
    return value
