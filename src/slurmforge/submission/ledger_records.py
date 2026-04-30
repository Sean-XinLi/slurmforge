from __future__ import annotations

from typing import Any

from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema
from .models import GroupSubmissionRecord, SubmissionLedger, SubmitGeneration

LEDGER_STATE_PLANNED = "planned"
LEDGER_STATE_SUBMITTING = "submitting"
LEDGER_STATE_PARTIAL = "partial"
LEDGER_STATE_SUBMITTED = "submitted"
LEDGER_STATE_FAILED = "failed"
LEDGER_STATE_UNCERTAIN = "uncertain"

GROUP_STATE_PLANNED = "planned"
GROUP_STATE_SUBMITTING = "submitting"
GROUP_STATE_SUBMITTED = "submitted"
GROUP_STATE_ADOPTED = "adopted"
GROUP_STATE_FAILED = "failed"

LEDGER_STATES = (
    LEDGER_STATE_PLANNED,
    LEDGER_STATE_SUBMITTING,
    LEDGER_STATE_PARTIAL,
    LEDGER_STATE_SUBMITTED,
    LEDGER_STATE_FAILED,
    LEDGER_STATE_UNCERTAIN,
)
GROUP_STATES = (
    GROUP_STATE_PLANNED,
    GROUP_STATE_SUBMITTING,
    GROUP_STATE_SUBMITTED,
    GROUP_STATE_ADOPTED,
    GROUP_STATE_FAILED,
)
GROUP_JOB_STATES = (GROUP_STATE_SUBMITTED, GROUP_STATE_ADOPTED)


def submission_ledger_from_dict(payload: dict[str, Any]) -> SubmissionLedger:
    version = require_schema(
        payload, name="submission_ledger", version=SchemaVersion.SUBMISSION_LEDGER
    )
    groups_raw = payload.get("groups")
    if not isinstance(groups_raw, dict) or not groups_raw:
        raise RecordContractError("submission_ledger.groups must be a non-empty object")
    ledger = SubmissionLedger(
        schema_version=version,
        batch_id=_required_string(payload, "batch_id"),
        stage_name=_required_string(payload, "stage_name"),
        generation_id=_required_string(payload, "generation_id"),
        state=_required_string(payload, "state"),
        groups={
            str(group_id): group_submission_record_from_dict(str(group_id), dict(group))
            for group_id, group in groups_raw.items()
        },
    )
    validate_submission_ledger(ledger)
    return ledger


def group_submission_record_from_dict(
    group_id: str, payload: dict[str, Any]
) -> GroupSubmissionRecord:
    payload_group_id = _required_string(payload, "group_id")
    if payload_group_id != group_id:
        raise RecordContractError(
            f"submission group key `{group_id}` does not match record `{payload_group_id}`"
        )
    record = GroupSubmissionRecord(
        group_id=group_id,
        sbatch_path=_required_string(payload, "sbatch_path"),
        dependency=_optional_string(payload, "dependency"),
        scheduler_job_id=_optional_string(payload, "scheduler_job_id"),
        state=_required_string(payload, "state"),
        submitted_at=_optional_string(payload, "submitted_at"),
        reason=str(payload.get("reason") or ""),
    )
    validate_group_submission_record(record)
    return record


def validate_submission_ledger(ledger: SubmissionLedger) -> None:
    if ledger.schema_version != SchemaVersion.SUBMISSION_LEDGER:
        raise RecordContractError("submission_ledger.schema_version is invalid")
    if not ledger.batch_id:
        raise RecordContractError("submission_ledger.batch_id is required")
    if not ledger.stage_name:
        raise RecordContractError("submission_ledger.stage_name is required")
    if not ledger.generation_id:
        raise RecordContractError("submission_ledger.generation_id is required")
    if ledger.state not in LEDGER_STATES:
        raise RecordContractError(
            f"Unsupported submission ledger state: {ledger.state}"
        )
    if not ledger.groups:
        raise RecordContractError("submission_ledger.groups must be non-empty")
    for group_id, group in ledger.groups.items():
        if group_id != group.group_id:
            raise RecordContractError(
                f"submission group key `{group_id}` does not match `{group.group_id}`"
            )
        validate_group_submission_record(group)


def validate_ledger_matches_generation(
    ledger: SubmissionLedger, generation: SubmitGeneration
) -> None:
    expected = set(generation.sbatch_paths_by_group)
    actual = set(ledger.groups)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise RecordContractError(
            "submission ledger groups do not match submit manifest: "
            f"missing={missing} extra={extra}"
        )
    for group_id, expected_sbatch in generation.sbatch_paths_by_group.items():
        actual_sbatch = ledger.groups[group_id].sbatch_path
        if actual_sbatch != expected_sbatch:
            raise RecordContractError(
                f"submission group `{group_id}` sbatch_path `{actual_sbatch}` "
                f"does not match manifest `{expected_sbatch}`"
            )


def validate_group_submission_record(record: GroupSubmissionRecord) -> None:
    if not record.group_id:
        raise RecordContractError("submission group group_id is required")
    if not record.sbatch_path:
        raise RecordContractError("submission group sbatch_path is required")
    if record.state not in GROUP_STATES:
        raise RecordContractError(f"Unsupported submission group state: {record.state}")
    if record.state in GROUP_JOB_STATES and not record.scheduler_job_id:
        raise RecordContractError(
            f"{record.state} submission group requires scheduler_job_id"
        )
    if record.state == GROUP_STATE_SUBMITTING:
        if record.scheduler_job_id:
            raise RecordContractError(
                "submitting submission group cannot already have scheduler_job_id"
            )
        if record.submitted_at:
            raise RecordContractError(
                "submitting submission group cannot already have submitted_at"
            )
    if record.state == GROUP_STATE_FAILED and not record.reason:
        raise RecordContractError("failed submission group requires reason")


def _required_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"submission_ledger.{field_name} must be a non-empty string"
        )
    return value


def _optional_string(payload: dict[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise RecordContractError(f"submission_ledger.{field_name} must be a string")
    return value
