"""Layout writers for stage batch roots."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..io import SchemaVersion, write_json
from ..lineage.builders import build_stage_batch_lineage
from ..lineage.paths import write_lineage_index
from ..plans.stage import StageBatchPlan
from .batch_materialization_records import write_materialization_status
from .paths import input_bindings_path, stage_plan_path


def _root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root)


def _bindings_payload(instance) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.INPUT_BINDINGS,
        "stage_instance_id": instance.stage_instance_id,
        "bindings": {
            binding.input_name: {
                "schema_version": binding.schema_version,
                "input_name": binding.input_name,
                "source": binding.source,
                "expects": binding.expects,
                "resolved": binding.resolved,
                "inject": binding.inject,
                "resolution": binding.resolution,
            }
            for binding in instance.input_bindings
        },
    }


def _persist_stage_run_layout(batch: StageBatchPlan, batch_root: Path) -> None:
    for instance in batch.stage_instances:
        run_dir = batch_root / instance.run_dir_rel
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(stage_plan_path(run_dir), instance)
        write_json(input_bindings_path(run_dir), _bindings_payload(instance))


def persist_stage_batch_layout(
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
    _persist_stage_run_layout(batch, batch_root)
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root


def persist_selected_stage_batch_layout(
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
    write_json(
        batch_root / "groups" / "selected_gpu_budget_plan.json", batch.budget_plan
    )
    write_json(
        batch_root / "blocked_runs.json",
        {
            "schema_version": SchemaVersion.BLOCKED_RUNS,
            "run_ids": sorted(blocked_run_ids or []),
        },
    )
    _persist_stage_run_layout(batch, batch_root)
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root
