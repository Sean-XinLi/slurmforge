from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..errors import RecordContractError
from ..io import (
    SchemaVersion,
    read_json_object,
    require_schema,
    to_jsonable,
    utc_now,
    write_json_object,
)
from ..record_fields import (
    required_string,
    required_string_tuple,
    string_tuple_record_field,
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
        event=required_string(
            payload, "event", label="notification", non_empty=True
        ),
        root_kind=required_string(
            payload, "root_kind", label="notification", non_empty=True
        ),
        root=required_string(payload, "root", label="notification", non_empty=True),
        backend=required_string(
            payload, "backend", label="notification", non_empty=True
        ),
        state=required_string(payload, "state", label="notification", non_empty=True),
        recipients=required_string_tuple(
            payload, "recipients", label="notification", non_empty_items=True
        ),
        scheduler_job_ids=required_string_tuple(
            payload, "scheduler_job_ids", label="notification", non_empty_items=True
        ),
        sbatch_paths=required_string_tuple(
            payload, "sbatch_paths", label="notification", non_empty_items=True
        ),
        barrier_job_ids=required_string_tuple(
            payload, "barrier_job_ids", label="notification", non_empty_items=True
        ),
        dependency_job_ids=required_string_tuple(
            payload, "dependency_job_ids", label="notification", non_empty_items=True
        ),
        dependency_type=required_string(
            payload, "dependency_type", label="notification", non_empty=True
        ),
        mail_type=required_string(
            payload, "mail_type", label="notification", non_empty=True
        ),
        submitted_at=required_string(payload, "submitted_at", label="notification"),
        reason=required_string(payload, "reason", label="notification"),
    )
    validate_notification_submission_record(record)
    return record


def read_notification_record(
    root: Path, event: str, backend: str = "slurm_mail"
) -> NotificationSubmissionRecord | None:
    path = notification_record_path(root, event, backend)
    if not path.exists():
        return None
    return notification_submission_record_from_dict(read_json_object(path))


def write_notification_record(root: Path, record: NotificationSubmissionRecord) -> None:
    validate_notification_submission_record(record)
    write_json_object(notification_record_path(root, record.event, record.backend), record)


def validate_notification_submission_record(record: NotificationSubmissionRecord) -> None:
    if record.schema_version != SchemaVersion.NOTIFICATION:
        raise RecordContractError("notification record schema_version is invalid")
    if record.state not in NOTIFICATION_STATES:
        raise RecordContractError(f"Unsupported notification state: {record.state}")
    for field_name in ("event", "root_kind", "root", "backend"):
        if not getattr(record, field_name):
            raise RecordContractError(f"notification.{field_name} is required")
    string_tuple_record_field(
        record.recipients,
        label="notification.recipients",
        non_empty=True,
        non_empty_items=True,
    )
    scheduler_job_ids = string_tuple_record_field(
        record.scheduler_job_ids,
        label="notification.scheduler_job_ids",
        non_empty_items=True,
    )
    sbatch_paths = string_tuple_record_field(
        record.sbatch_paths,
        label="notification.sbatch_paths",
        non_empty_items=True,
    )
    string_tuple_record_field(
        record.barrier_job_ids,
        label="notification.barrier_job_ids",
        non_empty_items=True,
    )
    string_tuple_record_field(
        record.dependency_job_ids,
        label="notification.dependency_job_ids",
        non_empty_items=True,
    )
    if not record.dependency_type:
        raise RecordContractError("notification.dependency_type is required")
    if not record.mail_type:
        raise RecordContractError("notification.mail_type is required")
    if record.state == "submitted":
        if not scheduler_job_ids:
            raise RecordContractError(
                "submitted notification requires scheduler_job_ids"
            )
        if not sbatch_paths:
            raise RecordContractError("submitted notification requires sbatch_paths")
    if record.state in {"failed", "uncertain"} and not record.reason:
        raise RecordContractError(f"{record.state} notification requires reason")
