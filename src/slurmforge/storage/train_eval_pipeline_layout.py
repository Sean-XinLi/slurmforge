"""Layout writer for train/eval pipeline roots."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..io import SchemaVersion, write_json
from ..lineage.builders import build_train_eval_pipeline_lineage
from ..lineage.paths import write_lineage_index
from ..plans.train_eval import TRAIN_EVAL_PIPELINE_KIND, TrainEvalPipelinePlan
from .batch_layout import write_stage_batch_layout


def write_train_eval_pipeline_layout(plan: TrainEvalPipelinePlan, *, spec_snapshot: dict[str, Any]) -> Path:
    root = Path(plan.root_dir)
    root.mkdir(parents=True, exist_ok=True)
    write_json(
        root / "manifest.json",
        {
            "schema_version": SchemaVersion.PIPELINE_MANIFEST,
            "kind": TRAIN_EVAL_PIPELINE_KIND,
            "pipeline_id": plan.pipeline_id,
            "pipeline_kind": plan.pipeline_kind,
            "stage_order": plan.stage_order,
            "spec_snapshot_digest": plan.spec_snapshot_digest,
        },
    )
    (root / "spec_snapshot.yaml").write_text(
        yaml.safe_dump(spec_snapshot, sort_keys=True),
        encoding="utf-8",
    )
    write_json(root / "train_eval_pipeline_plan.json", plan)
    for batch in plan.stage_batches.values():
        write_stage_batch_layout(batch, spec_snapshot=spec_snapshot, pipeline_root=root)
    write_lineage_index(root, build_train_eval_pipeline_lineage(plan))
    return root
