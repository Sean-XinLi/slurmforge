"""Layout writer for train/eval pipeline roots."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..io import SchemaVersion, write_json
from ..lineage.builders import build_train_eval_pipeline_lineage
from ..lineage.paths import write_lineage_index
from ..plans.train_eval import TrainEvalPipelinePlan
from ..workflow_contract import (
    BATCH_ROLE_PIPELINE_ENTRY,
    BATCH_ROLE_PIPELINE_STAGE,
    TRAIN_EVAL_PIPELINE_KIND,
)
from .batch_layout import persist_stage_batch_layout
from .execution_catalog import initialize_stage_catalog, upsert_catalog_batch
from .runtime_batches import initialize_runtime_batches, upsert_runtime_batch


def persist_train_eval_pipeline_layout(
    plan: TrainEvalPipelinePlan, *, spec_snapshot: dict[str, Any]
) -> Path:
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
    initialize_stage_catalog(root, pipeline_id=plan.pipeline_id)
    initialize_runtime_batches(root, pipeline_id=plan.pipeline_id)
    for stage_index, stage_name in enumerate(plan.stage_order):
        batch = plan.stage_batches[stage_name]
        persist_stage_batch_layout(
            batch, spec_snapshot=spec_snapshot, pipeline_root=root
        )
        upsert_catalog_batch(root, batch, role=BATCH_ROLE_PIPELINE_STAGE)
        if stage_index == 0:
            upsert_runtime_batch(
                root,
                batch,
                role=BATCH_ROLE_PIPELINE_ENTRY,
                shard_id="",
                source_train_group_id="",
            )
    write_lineage_index(root, build_train_eval_pipeline_lineage(plan))
    return root
