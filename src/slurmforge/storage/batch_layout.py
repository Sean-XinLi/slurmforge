"""Layout writers for stage batch roots."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..io import SchemaVersion, read_json, write_json
from ..lineage import build_stage_batch_lineage, write_lineage_index
from ..plans import TRAIN_EVAL_PIPELINE_KIND, StageBatchPlan
from .materialization import write_materialization_status
from .status_seed import seed_planned_stage_statuses


def _root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root)


def _pipeline_root_for_batch_root(batch_root: Path) -> Path | None:
    root = Path(batch_root).resolve()
    if root.parent.name != "stage_batches":
        return None
    candidate = root.parent.parent
    manifest = candidate / "manifest.json"
    if not manifest.exists():
        return None
    try:
        return candidate.resolve() if read_json(manifest).get("kind") == TRAIN_EVAL_PIPELINE_KIND else None
    except Exception:
        return None


def write_stage_batch_layout(
    batch: StageBatchPlan,
    *,
    spec_snapshot: dict[str, Any],
    pipeline_root: Path | None = None,
) -> Path:
    batch_root = _root(batch)
    batch_root.mkdir(parents=True, exist_ok=True)
    write_json(
        batch_root / "manifest.json",
        {
            "schema_version": SchemaVersion.BATCH_MANIFEST,
            "kind": "stage_batch",
            "batch_id": batch.batch_id,
            "stage_name": batch.stage_name,
            "project": batch.project,
            "experiment": batch.experiment,
            "spec_snapshot_digest": batch.spec_snapshot_digest,
        },
    )
    (batch_root / "spec_snapshot.yaml").write_text(
        yaml.safe_dump(spec_snapshot, sort_keys=True),
        encoding="utf-8",
    )
    write_json(batch_root / "batch_plan.json", batch)
    write_materialization_status(
        batch_root,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        state="planned",
    )
    write_json(
        batch_root / "groups" / "groups.json",
        {"schema_version": SchemaVersion.GROUPS, "groups": batch.group_plans},
    )
    write_json(batch_root / "groups" / "gpu_budget_plan.json", batch.budget_plan)
    seed_planned_stage_statuses(
        batch,
        batch_root,
        pipeline_root=pipeline_root or _pipeline_root_for_batch_root(batch_root),
    )
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root


def write_selected_stage_batch_layout(
    batch: StageBatchPlan,
    *,
    blocked_run_ids: list[str] | None = None,
) -> Path:
    batch_root = _root(batch)
    write_json(batch_root / "selected_batch_plan.json", batch)
    write_materialization_status(
        batch_root,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        state="planned",
    )
    write_json(
        batch_root / "groups" / "selected_groups.json",
        {"schema_version": SchemaVersion.GROUPS, "groups": batch.group_plans},
    )
    write_json(batch_root / "groups" / "selected_gpu_budget_plan.json", batch.budget_plan)
    write_json(
        batch_root / "blocked_runs.json",
        {
            "schema_version": SchemaVersion.BLOCKED_RUNS,
            "run_ids": sorted(blocked_run_ids or []),
        },
    )
    seed_planned_stage_statuses(
        batch,
        batch_root,
        pipeline_root=_pipeline_root_for_batch_root(batch_root),
    )
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root
