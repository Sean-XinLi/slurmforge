from __future__ import annotations

from pathlib import Path
from typing import Any

from ..plans.stage import StageBatchPlan
from ..root_model.seed import seed_planned_stage_statuses
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..storage.batch_layout import (
    persist_selected_stage_batch_layout,
    persist_stage_batch_layout,
)


def materialize_stage_batch(
    batch: StageBatchPlan,
    *,
    spec_snapshot: dict[str, Any],
    pipeline_root: Path | None = None,
) -> Path:
    root = persist_stage_batch_layout(
        batch,
        spec_snapshot=spec_snapshot,
        pipeline_root=pipeline_root,
    )
    seed_planned_stage_statuses(batch, root, pipeline_root=pipeline_root)
    refresh_stage_batch_status(root)
    return root


def materialize_selected_stage_batch(
    batch: StageBatchPlan,
    *,
    blocked_run_ids: list[str] | None = None,
    pipeline_root: Path,
) -> Path:
    root = persist_selected_stage_batch_layout(
        batch,
        blocked_run_ids=blocked_run_ids,
    )
    seed_planned_stage_statuses(batch, root, pipeline_root=pipeline_root)
    refresh_stage_batch_status(root)
    refresh_train_eval_pipeline_status(pipeline_root)
    return root
