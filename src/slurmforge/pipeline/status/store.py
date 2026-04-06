from __future__ import annotations

import json
from pathlib import Path

from ...errors import ConfigContractError
from ..records.io_utils import atomic_write_text
from .codecs import (
    deserialize_attempt_result,
    deserialize_execution_status,
    serialize_attempt_result,
    serialize_execution_status,
)
from .models import AttemptResult, ExecutionStatus
from .paths import latest_result_dir_pointer_path_for_run


def write_attempt_result(path: Path, attempt: AttemptResult) -> None:
    atomic_write_text(path, json.dumps(serialize_attempt_result(attempt), indent=2, sort_keys=True))


def write_execution_status(path: Path, status: ExecutionStatus) -> None:
    atomic_write_text(path, json.dumps(serialize_execution_status(status), indent=2, sort_keys=True))


def write_latest_result_dir(run_dir: Path, result_dir: Path) -> None:
    payload = {
        "schema_version": 1,
        "result_dir_rel": result_dir.resolve().relative_to(run_dir.resolve()).as_posix(),
    }
    atomic_write_text(
        latest_result_dir_pointer_path_for_run(run_dir),
        json.dumps(payload, indent=2, sort_keys=True),
    )


def read_execution_status(path: Path) -> ExecutionStatus | None:
    if not path.exists():
        return None
    return deserialize_execution_status(json.loads(path.read_text(encoding="utf-8")), result_dir=path.parent.parent)


def read_latest_result_dir(run_dir: Path) -> Path | None:
    path = latest_result_dir_pointer_path_for_run(run_dir)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"latest result pointer must be a mapping: {path}")
    rel_value = payload.get("result_dir_rel")
    if rel_value in {None, ""}:
        raise ConfigContractError(f"latest result pointer is missing required result_dir_rel: {path}")
    candidate = Path(str(rel_value))
    if candidate.is_absolute():
        raise ConfigContractError(f"latest result pointer must store a relative path: {path}")
    return (run_dir / candidate).resolve()


def read_attempt_result(path: Path) -> AttemptResult | None:
    if not path.exists():
        return None
    return deserialize_attempt_result(json.loads(path.read_text(encoding="utf-8")), result_dir=path.parent.parent)
