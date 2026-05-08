from __future__ import annotations

from pathlib import Path

from ..io import read_json
from ..plans.outputs import StageOutputsRecord
from ..plans.serde import stage_outputs_record_from_dict


def stage_outputs_path(run_dir: Path) -> Path:
    return Path(run_dir) / "stage_outputs.json"


def load_stage_outputs(run_dir: Path) -> StageOutputsRecord | None:
    """Read typed outputs record from disk, validating its schema."""
    path = stage_outputs_path(run_dir)
    if not path.exists():
        return None
    return stage_outputs_record_from_dict(read_json(path))
