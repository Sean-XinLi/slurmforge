from __future__ import annotations

from pathlib import Path

from .root_model.manifest import read_root_manifest
from .workflow_contract import TRAIN_EVAL_PIPELINE_KIND


def parent_pipeline_root_for_stage_batch(stage_batch_root: Path) -> Path | None:
    root = Path(stage_batch_root).resolve()
    for candidate in root.parents:
        manifest = read_root_manifest(candidate)
        if manifest is None:
            continue
        if manifest.kind == TRAIN_EVAL_PIPELINE_KIND:
            return candidate.resolve()
    return None
