from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, read_json, require_schema, utc_now, write_json


@dataclass(frozen=True)
class MaterializationStatusRecord:
    schema_version: int
    batch_id: str
    stage_name: str
    state: str
    failure_class: str | None = None
    reason: str = ""
    verified_at: str = ""
    submit_manifest_path: str = ""


def materialization_status_path(batch_root: Path) -> Path:
    return batch_root / "materialization_status.json"


def materialization_status_from_dict(payload: dict) -> MaterializationStatusRecord:
    version = require_schema(
        payload,
        name="materialization_status",
        version=SchemaVersion.MATERIALIZATION_STATUS,
    )
    return MaterializationStatusRecord(
        schema_version=version,
        batch_id=str(payload["batch_id"]),
        stage_name=str(payload["stage_name"]),
        state=str(payload.get("state") or "planned"),
        failure_class=None
        if payload.get("failure_class") in (None, "")
        else str(payload.get("failure_class")),
        reason=str(payload.get("reason") or ""),
        verified_at=str(payload.get("verified_at") or ""),
        submit_manifest_path=str(payload.get("submit_manifest_path") or ""),
    )


def read_materialization_status(batch_root: Path) -> MaterializationStatusRecord | None:
    path = materialization_status_path(batch_root)
    if not path.exists():
        return None
    return materialization_status_from_dict(read_json(path))


def write_materialization_status(
    batch_root: Path,
    *,
    batch_id: str,
    stage_name: str,
    state: str,
    failure_class: str | None = None,
    reason: str = "",
    submit_manifest_path: str = "",
    verified_at: str | None = None,
) -> MaterializationStatusRecord:
    record = MaterializationStatusRecord(
        schema_version=SchemaVersion.MATERIALIZATION_STATUS,
        batch_id=batch_id,
        stage_name=stage_name,
        state=state,
        failure_class=failure_class,
        reason=reason,
        verified_at=utc_now()
        if verified_at is None and state in {"verifying_inputs", "ready", "blocked"}
        else (verified_at or ""),
        submit_manifest_path=submit_manifest_path,
    )
    write_json(materialization_status_path(batch_root), record)
    return record
