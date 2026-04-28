from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import read_json
from ..plans.serde import stage_outputs_record_from_dict


def stage_outputs_path(run_dir: Path) -> Path:
    return Path(run_dir) / "stage_outputs.json"


def load_stage_outputs(run_dir: Path) -> dict[str, Any] | None:
    """Read raw outputs payload from disk, validating its schema."""
    path = stage_outputs_path(run_dir)
    if not path.exists():
        return None
    payload = read_json(path)
    stage_outputs_record_from_dict(payload)
    return payload
