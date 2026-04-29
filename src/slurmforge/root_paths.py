from __future__ import annotations

from pathlib import Path

from .io import read_json
from .plans.train_eval import TRAIN_EVAL_PIPELINE_KIND


def parent_pipeline_root_for_stage_batch(stage_batch_root: Path) -> Path | None:
    root = Path(stage_batch_root).resolve()
    if root.parent.name != "stage_batches":
        return None
    candidate = root.parent.parent
    manifest = candidate / "manifest.json"
    if not manifest.exists():
        return None
    try:
        payload = read_json(manifest)
    except Exception:
        return None
    return (
        candidate.resolve() if payload.get("kind") == TRAIN_EVAL_PIPELINE_KIND else None
    )
