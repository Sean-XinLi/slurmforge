from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import RecordContractError
from .io import SchemaVersion, read_json_object, require_schema, write_json_object
from .record_fields import required_string, required_string_map


@dataclass(frozen=True)
class GateTaskMapRecord:
    submission_id: str
    group_id: str
    tasks: dict[str, str]
    schema_version: int = SchemaVersion.CONTROL_SUBMISSIONS


def gate_task_map_from_dict(payload: dict[str, Any]) -> GateTaskMapRecord:
    require_schema(
        payload, name="gate_task_map", version=SchemaVersion.CONTROL_SUBMISSIONS
    )
    return GateTaskMapRecord(
        submission_id=required_string(
            payload, "submission_id", label="gate_task_map", non_empty=True
        ),
        group_id=required_string(
            payload, "group_id", label="gate_task_map", non_empty=True
        ),
        tasks=required_string_map(
            payload, "tasks", label="gate_task_map", non_empty_values=True
        ),
    )


def gate_task_map_to_dict(record: GateTaskMapRecord) -> dict[str, Any]:
    if not record.submission_id:
        raise RecordContractError("gate_task_map.submission_id must be non-empty")
    if not record.group_id:
        raise RecordContractError("gate_task_map.group_id must be non-empty")
    if not record.tasks:
        raise RecordContractError("gate_task_map.tasks must be non-empty")
    tasks: dict[str, str] = {}
    for key, value in record.tasks.items():
        if not isinstance(key, str) or not key:
            raise RecordContractError(
                "gate_task_map.tasks keys must be non-empty strings"
            )
        if not isinstance(value, str) or not value:
            raise RecordContractError(
                f"gate_task_map.tasks.{key} must be a non-empty string"
            )
        tasks[key] = value
    return {
        "schema_version": SchemaVersion.CONTROL_SUBMISSIONS,
        "submission_id": record.submission_id,
        "group_id": record.group_id,
        "tasks": tasks,
    }


def write_gate_task_map(path: Path, record: GateTaskMapRecord) -> None:
    write_json_object(path, gate_task_map_to_dict(record))


def load_gate_task_map(path: Path) -> GateTaskMapRecord:
    return gate_task_map_from_dict(read_json_object(path))


def stage_instance_id_for_task(record: GateTaskMapRecord, task_id: str) -> str:
    if task_id not in record.tasks:
        raise RecordContractError(
            f"gate_task_map.tasks does not contain SLURM_ARRAY_TASK_ID `{task_id}`"
        )
    return record.tasks[task_id]
