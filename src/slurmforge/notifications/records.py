from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..io import (
    SchemaVersion,
    read_json,
    require_schema,
    to_jsonable,
    utc_now,
    write_json,
)
from .models import NotificationSubmissionRecord


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
    require_schema(
        payload, name="notification_submission", version=SchemaVersion.NOTIFICATION
    )
    return NotificationSubmissionRecord(
        event=str(payload["event"]),
        root_kind=str(payload["root_kind"]),
        root=str(payload["root"]),
        backend=str(payload["backend"]),
        state=str(payload["state"]),
        recipients=tuple(str(item) for item in payload.get("recipients", ())),
        scheduler_job_ids=tuple(
            str(item) for item in payload.get("scheduler_job_ids", ())
        ),
        sbatch_paths=tuple(str(item) for item in payload.get("sbatch_paths", ())),
        barrier_job_ids=tuple(str(item) for item in payload.get("barrier_job_ids", ())),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids", ())
        ),
        dependency_type=str(payload.get("dependency_type") or ""),
        mail_type=str(payload.get("mail_type") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
        reason=str(payload.get("reason") or ""),
    )


def read_notification_record(
    root: Path, event: str, backend: str = "slurm_mail"
) -> NotificationSubmissionRecord | None:
    path = notification_record_path(root, event, backend)
    if not path.exists():
        return None
    return notification_submission_record_from_dict(read_json(path))


def write_notification_record(root: Path, record: NotificationSubmissionRecord) -> None:
    write_json(notification_record_path(root, record.event, record.backend), record)
