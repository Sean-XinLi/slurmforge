from __future__ import annotations

from pathlib import Path

from ..io import read_json_object
from ..plans.outputs import StageOutputsRecord
from ..plans.serde import stage_outputs_record_from_dict
from ..storage.paths import stage_outputs_path as _stage_outputs_path


def load_stage_outputs(run_dir: Path) -> StageOutputsRecord | None:
    """Read typed outputs record from disk, validating its schema."""
    path = _stage_outputs_path(run_dir)
    if not path.exists():
        return None
    return stage_outputs_record_from_dict(read_json_object(path))
