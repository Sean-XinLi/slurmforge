from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import RecordContractError
from ..io import SchemaVersion, read_json_object, require_schema, utc_now, write_json_object
from ..record_fields import required_nullable_string, required_string

MATERIALIZATION_PLANNED = "planned"
MATERIALIZATION_VERIFYING_INPUTS = "verifying_inputs"
MATERIALIZATION_READY = "ready"
MATERIALIZATION_BLOCKED = "blocked"
MATERIALIZATION_STATES = (
    MATERIALIZATION_PLANNED,
    MATERIALIZATION_VERIFYING_INPUTS,
    MATERIALIZATION_READY,
    MATERIALIZATION_BLOCKED,
)


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
        batch_id=required_string(
            payload, "batch_id", label="materialization_status", non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label="materialization_status", non_empty=True
        ),
        state=_materialization_state(payload),
        failure_class=required_nullable_string(
            payload, "failure_class", label="materialization_status"
        ),
        reason=required_string(payload, "reason", label="materialization_status"),
        verified_at=required_string(
            payload, "verified_at", label="materialization_status"
        ),
        submit_manifest_path=required_string(
            payload, "submit_manifest_path", label="materialization_status"
        ),
    )


def read_materialization_status(batch_root: Path) -> MaterializationStatusRecord | None:
    path = materialization_status_path(batch_root)
    if not path.exists():
        return None
    return materialization_status_from_dict(read_json_object(path))


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
    write_json_object(materialization_status_path(batch_root), record)
    return record


def _materialization_state(payload: dict) -> str:
    state = required_string(
        payload, "state", label="materialization_status", non_empty=True
    )
    if state not in MATERIALIZATION_STATES:
        raise RecordContractError(f"Unsupported materialization status state: {state}")
    return state
