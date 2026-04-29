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
from .models import NotificationDeliveryRecord


def notifications_dir(root: Path) -> Path:
    return Path(root) / "notifications"


def notification_records_dir(root: Path) -> Path:
    return notifications_dir(root) / "records"


def notification_record_path(root: Path, event: str, backend: str = "email") -> Path:
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


def notification_delivery_record_from_dict(
    payload: dict[str, Any],
) -> NotificationDeliveryRecord:
    require_schema(
        payload, name="notification_delivery", version=SchemaVersion.NOTIFICATION
    )
    return NotificationDeliveryRecord(
        event=str(payload["event"]),
        root_kind=str(payload["root_kind"]),
        root=str(payload["root"]),
        backend=str(payload["backend"]),
        state=str(payload["state"]),
        recipients=tuple(str(item) for item in payload.get("recipients", ())),
        subject=str(payload.get("subject") or ""),
        sent_at=str(payload.get("sent_at") or ""),
        reason=str(payload.get("reason") or ""),
        scheduler_job_id=str(payload.get("scheduler_job_id") or ""),
        sbatch_path=str(payload.get("sbatch_path") or ""),
        barrier_job_ids=tuple(str(item) for item in payload.get("barrier_job_ids", ())),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids", ())
        ),
        submitted_at=str(payload.get("submitted_at") or ""),
    )


def read_notification_record(
    root: Path, event: str, backend: str = "email"
) -> NotificationDeliveryRecord | None:
    path = notification_record_path(root, event, backend)
    if not path.exists():
        return None
    return notification_delivery_record_from_dict(read_json(path))


def write_notification_record(root: Path, record: NotificationDeliveryRecord) -> None:
    write_json(notification_record_path(root, record.event, record.backend), record)
