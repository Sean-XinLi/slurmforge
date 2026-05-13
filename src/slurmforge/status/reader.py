from __future__ import annotations

from pathlib import Path

from ..io import read_json_object
from ..storage.paths import attempts_dir, status_path
from .models import StageAttemptRecord, StageStatusRecord
from .serde import attempt_from_dict, stage_status_from_dict


def read_stage_status(run_dir: Path) -> StageStatusRecord | None:
    path = status_path(run_dir)
    if not path.exists():
        return None
    return stage_status_from_dict(read_json_object(path))


def list_attempts(run_dir: Path) -> list[StageAttemptRecord]:
    root = attempts_dir(run_dir)
    if not root.exists():
        return []
    return [
        attempt_from_dict(read_json_object(path))
        for path in sorted(root.glob("*/attempt.json"))
    ]
