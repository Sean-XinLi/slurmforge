from __future__ import annotations

from ..io import content_digest
from ..contracts import RunDefinition
from ..spec import ExperimentSpec


def batch_id(stage_name: str, runs: tuple[RunDefinition, ...], source_ref: str, spec_digest: str) -> str:
    payload = {
        "stage_name": stage_name,
        "run_ids": [run.run_id for run in runs],
        "source_ref": source_ref,
        "spec_snapshot_digest": spec_digest,
    }
    digest = content_digest(payload, prefix=12)
    return f"{stage_name}_batch_{digest}"


def train_eval_pipeline_id(spec: ExperimentSpec, runs: tuple[RunDefinition, ...], stage_order: tuple[str, ...]) -> str:
    payload = {
        "pipeline_kind": "train_eval_pipeline",
        "stage_order": stage_order,
        "run_ids": [run.run_id for run in runs],
        "spec_snapshot_digest": spec.spec_snapshot_digest,
    }
    digest = content_digest(payload, prefix=12)
    return f"train_eval_pipeline_{digest}"
