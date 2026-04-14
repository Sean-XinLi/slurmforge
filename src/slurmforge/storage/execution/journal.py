"""Atomic read/write primitives for execution journal files.

This module owns the on-disk I/O for all execution-phase files.  Backends
and lifecycle import from here — not from ``pipeline.status.store``,
``pipeline.checkpoints.store``, etc.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ...errors import ConfigContractError
from ...pipeline.checkpoints.codec import deserialize_checkpoint_state, serialize_checkpoint_state
from ...pipeline.checkpoints.models import CheckpointState
from ...pipeline.status.codecs import (
    deserialize_attempt_result,
    deserialize_execution_status,
    serialize_attempt_result,
    serialize_execution_status,
)
from ...pipeline.status.models import AttemptResult, ExecutionStatus
from ...pipeline.train_outputs.codec import deserialize_train_outputs_manifest, serialize_train_outputs_manifest
from ...pipeline.train_outputs.models import TrainOutputsManifest
from .paths import (
    attempt_result_path,
    artifact_manifest_path,
    checkpoint_state_path,
    execution_status_path,
    latest_result_dir_pointer_path,
    train_outputs_manifest_path,
)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


# ---------------------------------------------------------------------------
# Execution status
# ---------------------------------------------------------------------------

def write_execution_status(result_dir: Path, status: ExecutionStatus) -> None:
    path = execution_status_path(result_dir)
    _atomic_write_text(path, json.dumps(serialize_execution_status(status), indent=2, sort_keys=True))


def read_execution_status(result_dir: Path) -> ExecutionStatus | None:
    path = execution_status_path(result_dir)
    if not path.exists():
        return None
    return deserialize_execution_status(
        json.loads(path.read_text(encoding="utf-8")),
        result_dir=result_dir,
    )


# ---------------------------------------------------------------------------
# Attempt result
# ---------------------------------------------------------------------------

def write_attempt_result(result_dir: Path, attempt: AttemptResult) -> None:
    path = attempt_result_path(result_dir)
    _atomic_write_text(path, json.dumps(serialize_attempt_result(attempt), indent=2, sort_keys=True))


def read_attempt_result(result_dir: Path) -> AttemptResult | None:
    path = attempt_result_path(result_dir)
    if not path.exists():
        return None
    return deserialize_attempt_result(
        json.loads(path.read_text(encoding="utf-8")),
        result_dir=result_dir,
    )


# ---------------------------------------------------------------------------
# Latest result dir pointer
# ---------------------------------------------------------------------------

def write_latest_result_dir(run_dir: Path, result_dir: Path) -> None:
    payload = {
        "schema_version": 1,
        "result_dir_rel": result_dir.resolve().relative_to(run_dir.resolve()).as_posix(),
    }
    _atomic_write_text(
        latest_result_dir_pointer_path(run_dir),
        json.dumps(payload, indent=2, sort_keys=True),
    )


def read_latest_result_dir(run_dir: Path) -> Path | None:
    path = latest_result_dir_pointer_path(run_dir)
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


# ---------------------------------------------------------------------------
# Checkpoint state
# ---------------------------------------------------------------------------

def write_checkpoint_state(result_dir: Path, state: CheckpointState) -> None:
    path = checkpoint_state_path(result_dir)
    _atomic_write_text(path, json.dumps(serialize_checkpoint_state(state), indent=2, sort_keys=True))


def read_checkpoint_state(result_dir: Path) -> CheckpointState | None:
    path = checkpoint_state_path(result_dir)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Invalid checkpoint state payload: {path}")
    return deserialize_checkpoint_state(payload)


# ---------------------------------------------------------------------------
# Train outputs manifest
# ---------------------------------------------------------------------------

def write_train_outputs_manifest(result_dir: Path, manifest: TrainOutputsManifest) -> None:
    path = train_outputs_manifest_path(result_dir)
    _atomic_write_text(path, json.dumps(serialize_train_outputs_manifest(manifest), indent=2, sort_keys=True))


def read_train_outputs_manifest(result_dir: Path) -> TrainOutputsManifest | None:
    path = train_outputs_manifest_path(result_dir)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return deserialize_train_outputs_manifest(payload)


# ---------------------------------------------------------------------------
# Artifact manifest
# ---------------------------------------------------------------------------

def write_artifact_manifest(result_dir: Path, manifest: dict[str, Any]) -> None:
    path = artifact_manifest_path(result_dir)
    _atomic_write_text(path, json.dumps(manifest, indent=2, sort_keys=True))


def read_artifact_manifest(result_dir: Path) -> dict[str, Any] | None:
    path = artifact_manifest_path(result_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
