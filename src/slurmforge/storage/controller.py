from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..errors import RecordContractError
from ..io import SchemaVersion, read_json, utc_now, write_json
from .paths import controller_events_path, controller_job_path, controller_status_path


_JOB_KEYS = {
    "schema_version",
    "pipeline_id",
    "scheduler_job_id",
    "submitted_at",
    "sbatch_path",
}


@dataclass(frozen=True)
class ControllerJobRecord:
    schema_version: int
    pipeline_id: str
    scheduler_job_id: str
    submitted_at: str
    sbatch_path: str


def append_controller_event(pipeline_root: Path, event: str, **payload: Any) -> None:
    path = controller_events_path(pipeline_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "at": utc_now(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def controller_job_record_from_dict(payload: dict[str, Any]) -> ControllerJobRecord:
    unexpected = set(payload) - _JOB_KEYS
    if unexpected:
        raise RecordContractError(
            f"controller_job.json has unexpected mutable fields: {sorted(unexpected)}"
        )
    missing = _JOB_KEYS - set(payload)
    if missing:
        raise RecordContractError(
            f"controller_job.json is missing required fields: {sorted(missing)}"
        )
    version = int(payload["schema_version"])
    if version != SchemaVersion.CONTROLLER_JOB:
        raise RecordContractError(
            f"controller_job.schema_version is not supported: {version}"
        )
    return ControllerJobRecord(
        schema_version=version,
        pipeline_id=str(payload["pipeline_id"]),
        scheduler_job_id=str(payload.get("scheduler_job_id") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
        sbatch_path=str(payload.get("sbatch_path") or ""),
    )


def read_controller_job(pipeline_root: Path) -> ControllerJobRecord | None:
    path = controller_job_path(pipeline_root)
    if not path.exists():
        return None
    return controller_job_record_from_dict(read_json(path))


def write_controller_job(
    pipeline_root: Path,
    record: ControllerJobRecord,
) -> ControllerJobRecord:
    path = controller_job_path(pipeline_root)
    payload = asdict(record)
    if path.exists():
        current = read_json(path)
        if current != payload:
            raise RuntimeError(
                f"controller job record is immutable and already exists: {path}"
            )
        return record
    write_json(path, record)
    return record


def read_controller_status(pipeline_root: Path) -> dict[str, Any] | None:
    path = controller_status_path(pipeline_root)
    if not path.exists():
        return None
    return read_json(path)


def write_controller_status(pipeline_root: Path, state: str, **payload: Any) -> None:
    job = read_controller_job(pipeline_root)
    if job is not None:
        payload.setdefault("scheduler_job_id", job.scheduler_job_id)
        payload.setdefault("sbatch_path", job.sbatch_path)
    write_json(
        controller_status_path(pipeline_root),
        {
            "schema_version": SchemaVersion.CONTROLLER_STATUS,
            "state": state,
            **payload,
        },
    )
