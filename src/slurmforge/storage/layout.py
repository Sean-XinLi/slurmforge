"""Layout writers for stage_batch / pipeline roots.

Layout writers persist plan files, materialization status, lineage indexes,
and seed initial ``planned`` stage statuses through ``status.commit_stage_status``.
Parent rollups are storage-owned and happen after each layout batch is written.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..io import SchemaVersion, read_json, write_json
from ..lineage import build_pipeline_lineage, build_stage_batch_lineage, write_lineage_index
from ..plans import (
    PipelinePlan,
    StageBatchPlan,
    StageInstancePlan,
)
from ..status import (
    StageStatusRecord,
    batched_commits,
    commit_stage_status,
    write_root_ref,
)
from .aggregate import refresh_pipeline_status, refresh_stage_batch_status
from .materialization import write_materialization_status
from .paths import input_bindings_path, stage_plan_path


def _root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root)


def _run_dir(batch_root: Path, instance: StageInstancePlan) -> Path:
    return batch_root / instance.run_dir_rel


def _pipeline_root_for_batch_root(batch_root: Path) -> Path | None:
    root = Path(batch_root).resolve()
    if root.parent.name != "stage_batches":
        return None
    candidate = root.parent.parent
    manifest = candidate / "manifest.json"
    if not manifest.exists():
        return None
    try:
        return candidate.resolve() if read_json(manifest).get("kind") == "pipeline" else None
    except Exception:
        return None


def _bindings_payload(instance: StageInstancePlan) -> dict[str, Any]:
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


def _seed_planned_statuses(
    batch: StageBatchPlan, batch_root: Path, *, pipeline_root: Path | None
) -> None:
    with batched_commits():
        for instance in batch.stage_instances:
            run_dir = _run_dir(batch_root, instance)
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json(stage_plan_path(run_dir), instance)
            write_json(input_bindings_path(run_dir), _bindings_payload(instance))
            write_root_ref(
                run_dir,
                stage_batch_root=batch_root,
                pipeline_root=pipeline_root,
            )
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=instance.stage_instance_id,
                    run_id=instance.run_id,
                    stage_name=instance.stage_name,
                    state="planned",
                ),
                source="layout",
            )
            (run_dir / "attempts").mkdir(exist_ok=True)


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
    write_json(batch_root / "groups" / "groups.json", {"schema_version": SchemaVersion.GROUPS, "groups": batch.group_plans})
    write_json(batch_root / "groups" / "gpu_budget_plan.json", batch.budget_plan)
    _seed_planned_statuses(
        batch,
        batch_root,
        pipeline_root=pipeline_root or _pipeline_root_for_batch_root(batch_root),
    )
    refresh_stage_batch_status(batch_root)
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root


def write_selected_stage_batch_layout(
    batch: StageBatchPlan, *, blocked_run_ids: list[str] | None = None
) -> Path:
    batch_root = _root(batch)
    write_json(batch_root / "selected_batch_plan.json", batch)
    write_materialization_status(
        batch_root,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        state="planned",
    )
    write_json(batch_root / "groups" / "selected_groups.json", {"schema_version": SchemaVersion.GROUPS, "groups": batch.group_plans})
    write_json(batch_root / "groups" / "selected_gpu_budget_plan.json", batch.budget_plan)
    write_json(
        batch_root / "blocked_runs.json",
        {
            "schema_version": SchemaVersion.BLOCKED_RUNS,
            "run_ids": sorted(blocked_run_ids or []),
        },
    )
    _seed_planned_statuses(
        batch, batch_root, pipeline_root=_pipeline_root_for_batch_root(batch_root)
    )
    refresh_stage_batch_status(batch_root)
    write_lineage_index(batch_root, build_stage_batch_lineage(batch))
    return batch_root


def write_pipeline_layout(plan: PipelinePlan, *, spec_snapshot: dict[str, Any]) -> Path:
    root = Path(plan.root_dir)
    root.mkdir(parents=True, exist_ok=True)
    write_json(
        root / "manifest.json",
        {
            "schema_version": SchemaVersion.PIPELINE_MANIFEST,
            "kind": "pipeline",
            "pipeline_id": plan.pipeline_id,
            "stage_order": plan.stage_order,
            "spec_snapshot_digest": plan.spec_snapshot_digest,
        },
    )
    (root / "spec_snapshot.yaml").write_text(
        yaml.safe_dump(spec_snapshot, sort_keys=True),
        encoding="utf-8",
    )
    write_json(root / "pipeline_plan.json", plan)
    write_json(root / "controller" / "controller_plan.json", plan.controller_plan)
    write_json(
        root / "controller" / "controller_state.json",
        {
            "schema_version": SchemaVersion.CONTROLLER_STATE,
            "pipeline_id": plan.pipeline_id,
            "state": "planned",
            "current_stage": plan.stage_order[0] if plan.stage_order else None,
            "completed_stages": [],
            "materialized_stages": [],
        },
    )
    write_json(root / "controller" / "controller_status.json", {"schema_version": SchemaVersion.CONTROLLER_STATUS, "state": "planned"})
    (root / "controller" / "events.jsonl").touch()
    for batch in plan.stage_batches.values():
        write_stage_batch_layout(batch, spec_snapshot=spec_snapshot, pipeline_root=root)
    refresh_pipeline_status(root)
    write_lineage_index(root, build_pipeline_lineage(plan))
    return root
