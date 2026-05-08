from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import ConfigContractError, RecordContractError
from .record_fields import (
    required_string,
    required_string_tuple,
    string_tuple_record_field,
)

CONTROL_KIND_STAGE_INSTANCE_GATE = "stage_instance_gate"
CONTROL_KIND_DISPATCH_CATCHUP_GATE = "dispatch_catchup_gate"
CONTROL_KIND_TERMINAL_NOTIFICATION = "terminal_notification"

CONTROL_KINDS = (
    CONTROL_KIND_STAGE_INSTANCE_GATE,
    CONTROL_KIND_DISPATCH_CATCHUP_GATE,
    CONTROL_KIND_TERMINAL_NOTIFICATION,
)

CONTROL_STATE_SUBMITTING = "submitting"
CONTROL_STATE_SUBMITTED = "submitted"
CONTROL_STATE_UNCERTAIN = "uncertain"
CONTROL_STATE_FAILED = "failed"
CONTROL_STATES = (
    CONTROL_STATE_SUBMITTING,
    CONTROL_STATE_SUBMITTED,
    CONTROL_STATE_UNCERTAIN,
    CONTROL_STATE_FAILED,
)


@dataclass(frozen=True)
class ControlJobRecord:
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


def control_job_key(kind: str, *, target_id: str) -> str:
    if not target_id:
        raise ConfigContractError(f"Control job `{kind}` requires target_id")
    return f"{kind}:{target_id}"


def control_job_from_payload(
    key: str,
    payload: dict[str, Any],
    *,
    label: str,
) -> ControlJobRecord:
    payload_key = required_string(payload, "key", label=label, non_empty=True)
    if payload_key != key:
        raise RecordContractError(
            f"{label} key `{payload_key}` does not match map key `{key}`"
        )
    record = ControlJobRecord(
        key=key,
        kind=required_string(payload, "kind", label=label, non_empty=True),
        target_kind=required_string(
            payload, "target_kind", label=label, non_empty=True
        ),
        target_id=required_string(payload, "target_id", label=label, non_empty=True),
        state=required_string(payload, "state", label=label, non_empty=True),
        sbatch_paths=required_string_tuple(
            payload, "sbatch_paths", label=label, non_empty_items=True
        ),
        scheduler_job_ids=required_string_tuple(
            payload, "scheduler_job_ids", label=label, non_empty_items=True
        ),
        barrier_job_ids=required_string_tuple(
            payload, "barrier_job_ids", label=label, non_empty_items=True
        ),
        dependency_job_ids=required_string_tuple(
            payload, "dependency_job_ids", label=label, non_empty_items=True
        ),
        reason=required_string(payload, "reason", label=label),
        started_at=required_string(payload, "started_at", label=label),
        submitted_at=required_string(payload, "submitted_at", label=label),
        failed_at=required_string(payload, "failed_at", label=label),
    )
    validate_control_job_record(record, label=label)
    return record


def validate_control_job_record(record: ControlJobRecord, *, label: str) -> None:
    if record.kind not in CONTROL_KINDS:
        raise RecordContractError(f"Unsupported {label} kind: {record.kind}")
    if record.state not in CONTROL_STATES:
        raise RecordContractError(f"Unsupported {label} state: {record.state}")
    if not record.target_kind:
        raise RecordContractError(f"{label} target_kind is required")
    if not record.target_id:
        raise RecordContractError(f"{label} target_id is required")
    expected_key = control_job_key(record.kind, target_id=record.target_id)
    if record.key != expected_key:
        raise RecordContractError(
            f"{label} key `{record.key}` does not match `{expected_key}`"
        )
    string_tuple_record_field(
        record.sbatch_paths,
        label=f"{label} sbatch_paths",
        non_empty=True,
        non_empty_items=True,
    )
    scheduler_job_ids = string_tuple_record_field(
        record.scheduler_job_ids,
        label=f"{label} scheduler_job_ids",
        non_empty_items=True,
    )
    string_tuple_record_field(
        record.barrier_job_ids,
        label=f"{label} barrier_job_ids",
        non_empty_items=True,
    )
    string_tuple_record_field(
        record.dependency_job_ids,
        label=f"{label} dependency_job_ids",
        non_empty_items=True,
    )
    if record.state == CONTROL_STATE_SUBMITTED and not scheduler_job_ids:
        raise RecordContractError(f"submitted {label} requires scheduler_job_ids")
    if record.state in {CONTROL_STATE_FAILED, CONTROL_STATE_UNCERTAIN}:
        if not record.reason:
            raise RecordContractError(f"{record.state} {label} requires a reason")


__all__ = (
    "CONTROL_KIND_DISPATCH_CATCHUP_GATE",
    "CONTROL_KIND_STAGE_INSTANCE_GATE",
    "CONTROL_KIND_TERMINAL_NOTIFICATION",
    "CONTROL_KINDS",
    "CONTROL_STATE_FAILED",
    "CONTROL_STATE_SUBMITTED",
    "CONTROL_STATE_SUBMITTING",
    "CONTROL_STATE_UNCERTAIN",
    "CONTROL_STATES",
    "ControlJobRecord",
    "control_job_from_payload",
    "control_job_key",
    "validate_control_job_record",
)
