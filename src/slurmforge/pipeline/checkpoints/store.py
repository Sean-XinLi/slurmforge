from __future__ import annotations

import json
from pathlib import Path

from ..records.io_utils import atomic_write_text
from .codec import deserialize_checkpoint_state, serialize_checkpoint_state
from .models import CheckpointState


def checkpoint_state_path_for_result_dir(result_dir: Path) -> Path:
    return result_dir / "meta" / "checkpoint_state.json"


def read_checkpoint_state(result_dir: Path) -> CheckpointState | None:
    path = checkpoint_state_path_for_result_dir(result_dir)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Invalid checkpoint state payload: {path}")
    return deserialize_checkpoint_state(payload)


def write_checkpoint_state(result_dir: Path, state: CheckpointState) -> Path:
    path = checkpoint_state_path_for_result_dir(result_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(path, json.dumps(serialize_checkpoint_state(state), indent=2, sort_keys=True))
    return path
