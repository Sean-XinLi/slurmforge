from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..errors import RecordContractError
from ..io import (
    SchemaVersion,
    read_json,
    require_schema,
    to_jsonable,
    utc_now,
    write_json,
)
from .models import NotificationSubmissionRecord

NOTIFICATION_STATES = ("submitted", "failed", "uncertain")


def notifications_dir(root: Path) -> Path:
    return Path(root) / "notifications"


def notification_records_dir(root: Path) -> Path:
    return notifications_dir(root) / "records"


def notification_record_path(
    root: Path, event: str, backend: str = "slurm_mail"
) -> Path:
    safe_event = event.replace("/", "_")
    safe_backend = backend.replace("/", "_")
    return notification_records_dir(root) / f"{safe_event}.{safe_backend}.json"


def notification_events_path(root: Path) -> Path:
    return notifications_dir(root) / "events.jsonl"


def append_notification_event(root: Path, event: str, **payload: Any) -> None:
    path = notification_events_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "at": utc_now(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(record), sort_keys=True) + "\n")


def notification_submission_record_from_dict(
    payload: dict[str, Any],
) -> NotificationSubmissionRecord:
    version = require_schema(
        payload, name="notification_submission", version=SchemaVersion.NOTIFICATION
    )
    record = NotificationSubmissionRecord(
        schema_version=version,
        event=_required_string(payload, "event"),
        root_kind=_required_string(payload, "root_kind"),
        root=_required_string(payload, "root"),
        backend=_required_string(payload, "backend"),
        state=_required_string(payload, "state"),
        recipients=_tuple_field(payload, "recipients", non_empty_items=True),
        scheduler_job_ids=_tuple_field(payload, "scheduler_job_ids"),
        sbatch_paths=_tuple_field(payload, "sbatch_paths"),
        barrier_job_ids=_tuple_field(payload, "barrier_job_ids"),
        dependency_job_ids=_tuple_field(payload, "dependency_job_ids"),
        dependency_type=_required_string(payload, "dependency_type"),
        mail_type=_required_string(payload, "mail_type"),
        submitted_at=str(payload.get("submitted_at") or ""),
        reason=str(payload.get("reason") or ""),
    )
    validate_notification_submission_record(record)
    return record


def read_notification_record(
    root: Path, event: str, backend: str = "slurm_mail"
) -> NotificationSubmissionRecord | None:
    path = notification_record_path(root, event, backend)
    if not path.exists():
        return None
    return notification_submission_record_from_dict(read_json(path))


def write_notification_record(root: Path, record: NotificationSubmissionRecord) -> None:
    validate_notification_submission_record(record)
    write_json(notification_record_path(root, record.event, record.backend), record)


def validate_notification_submission_record(record: NotificationSubmissionRecord) -> None:
    if record.schema_version != SchemaVersion.NOTIFICATION:
        raise RecordContractError("notification record schema_version is invalid")
    if record.state not in NOTIFICATION_STATES:
        raise RecordContractError(f"Unsupported notification state: {record.state}")
    for field_name in ("event", "root_kind", "root", "backend"):
        if not getattr(record, field_name):
            raise RecordContractError(f"notification.{field_name} is required")
    if not record.recipients or any(not recipient for recipient in record.recipients):
        raise RecordContractError("notification.recipients must be non-empty strings")
    if not record.dependency_type:
        raise RecordContractError("notification.dependency_type is required")
    if not record.mail_type:
        raise RecordContractError("notification.mail_type is required")
    if record.state == "submitted":
        if not record.scheduler_job_ids:
            raise RecordContractError(
                "submitted notification requires scheduler_job_ids"
            )
        if not record.sbatch_paths:
            raise RecordContractError("submitted notification requires sbatch_paths")
    if record.state in {"failed", "uncertain"} and not record.reason:
        raise RecordContractError(f"{record.state} notification requires reason")


def _required_string(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            f"notification.{field_name} must be a non-empty string"
        )
    return value


def _tuple_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    non_empty_items: bool = False,
) -> tuple[str, ...]:
    value = payload.get(field_name)
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise RecordContractError(f"notification.{field_name} must be an array")
    result = tuple(str(item) for item in value)
    if non_empty_items and any(not item for item in result):
        raise RecordContractError(
            f"notification.{field_name} must contain non-empty strings"
        )
    return result
