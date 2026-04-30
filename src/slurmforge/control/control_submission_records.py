from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..errors import ConfigContractError, RecordContractError
from ..io import SchemaVersion, utc_now

CONTROL_KIND_STAGE_INSTANCE_GATE = "stage_instance_gate"
CONTROL_KIND_DISPATCH_CATCHUP_GATE = "dispatch_catchup_gate"
CONTROL_KIND_TERMINAL_NOTIFICATION = "terminal_notification"

CONTROL_STATE_SUBMITTING = "submitting"
CONTROL_STATE_SUBMITTED = "submitted"
CONTROL_STATE_UNCERTAIN = "uncertain"
CONTROL_STATE_FAILED = "failed"

CONTROL_ON_ERROR_RAISE_UNCERTAIN = "raise_uncertain"
CONTROL_ON_ERROR_RECORD_FAILED = "record_failed"

CONTROL_KINDS = (
    CONTROL_KIND_STAGE_INSTANCE_GATE,
    CONTROL_KIND_DISPATCH_CATCHUP_GATE,
    CONTROL_KIND_TERMINAL_NOTIFICATION,
)
CONTROL_STATES = (
    CONTROL_STATE_SUBMITTING,
    CONTROL_STATE_SUBMITTED,
    CONTROL_STATE_UNCERTAIN,
    CONTROL_STATE_FAILED,
)
CONTROL_ON_ERROR_POLICIES = (
    CONTROL_ON_ERROR_RAISE_UNCERTAIN,
    CONTROL_ON_ERROR_RECORD_FAILED,
)


@dataclass(frozen=True)
class ControlSubmitResult:
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    state: str = CONTROL_STATE_SUBMITTED
    reason: str = ""


@dataclass
class ControlSubmissionRecord:
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
class ControlSubmissionLedger:
    schema_version: int
    updated_at: str
    submissions: dict[str, ControlSubmissionRecord] = field(default_factory=dict)


def control_submission_key(kind: str, *, target_id: str) -> str:
    if not target_id:
        raise ConfigContractError(f"Control submission `{kind}` requires target_id")
    return f"{kind}:{target_id}"


def empty_control_submission_ledger() -> ControlSubmissionLedger:
    return ControlSubmissionLedger(
        schema_version=SchemaVersion.CONTROL_SUBMISSIONS,
        updated_at=utc_now(),
    )


def control_submission_from_payload(
    key: str, payload: dict[str, Any]
) -> ControlSubmissionRecord:
    payload_key = _required_string(payload, "key")
    if payload_key != key:
        raise RecordContractError(
            f"control submission payload key `{payload_key}` does not match map key `{key}`"
        )
    kind = _required_string(payload, "kind")
    target_kind = _required_string(payload, "target_kind")
    target_id = _required_string(payload, "target_id")
    state = _required_string(payload, "state")
    sbatch_paths = _tuple_field(payload, "sbatch_paths")
    scheduler_job_ids = _tuple_field(payload, "scheduler_job_ids")
    barrier_job_ids = _tuple_field(payload, "barrier_job_ids")
    dependency_job_ids = _tuple_field(payload, "dependency_job_ids")
    reason = str(payload.get("reason") or "")
    validate_control_record(
        key=key,
        kind=kind,
        target_kind=target_kind,
        target_id=target_id,
        state=state,
        sbatch_paths=sbatch_paths,
        scheduler_job_ids=scheduler_job_ids,
        reason=reason,
    )
    return ControlSubmissionRecord(
        key=key,
        kind=kind,
        target_kind=target_kind,
        target_id=target_id,
        state=state,
        sbatch_paths=sbatch_paths,
        scheduler_job_ids=scheduler_job_ids,
        barrier_job_ids=barrier_job_ids,
        dependency_job_ids=dependency_job_ids,
        reason=reason,
        started_at=str(payload.get("started_at") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
        failed_at=str(payload.get("failed_at") or ""),
    )


def validate_control_record(
    *,
    key: str,
    kind: str,
    target_kind: str,
    target_id: str,
    state: str,
    sbatch_paths: tuple[str, ...],
    scheduler_job_ids: tuple[str, ...],
    reason: str,
) -> None:
    if kind not in CONTROL_KINDS:
        raise RecordContractError(f"Unsupported control submission kind: {kind}")
    if state not in CONTROL_STATES:
        raise RecordContractError(f"Unsupported control submission state: {state}")
    if not target_kind:
        raise RecordContractError("control submission target_kind is required")
    if not target_id:
        raise RecordContractError("control submission target_id is required")
    expected_key = control_submission_key(kind, target_id=target_id)
    if key != expected_key:
        raise RecordContractError(
            f"control submission key `{key}` does not match `{expected_key}`"
        )
    if not sbatch_paths:
        raise RecordContractError("control submission sbatch_paths must be non-empty")
    if state == CONTROL_STATE_SUBMITTED and not scheduler_job_ids:
        raise RecordContractError(
            "submitted control submission requires scheduler_job_ids"
        )
    if state in {CONTROL_STATE_FAILED, CONTROL_STATE_UNCERTAIN} and not reason:
        raise RecordContractError(
            f"{state} control submission requires a failure reason"
        )


def _required_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"control_submissions record field `{field_name}` must be a non-empty string"
        )
    return value


def _tuple_field(payload: dict[str, Any], field_name: str) -> tuple[str, ...]:
    value = payload.get(field_name)
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise RecordContractError(
            f"control_submissions record field `{field_name}` must be an array"
        )
    return tuple(str(item) for item in value)
