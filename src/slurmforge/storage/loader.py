"""Read-only helpers for stage_batch / pipeline roots."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..io import read_json
from ..plans import (
    PipelinePlan,
    RunDefinition,
    StageBatchPlan,
    StageInstancePlan,
    pipeline_plan_from_dict,
    stage_batch_plan_from_dict,
    stage_outputs_record_from_dict,
    stage_instance_plan_from_dict,
)
from ..status import StageStatusRecord, read_stage_status
from .paths import stage_outputs_path, stage_plan_path


def load_stage_batch_plan(batch_root: Path) -> StageBatchPlan:
    return stage_batch_plan_from_dict(read_json(batch_root / "batch_plan.json"))


def load_execution_stage_batch_plan(batch_root: Path) -> StageBatchPlan:
    selected = batch_root / "selected_batch_plan.json"
    if selected.exists():
        return stage_batch_plan_from_dict(read_json(selected))
    return load_stage_batch_plan(batch_root)


def load_pipeline_plan(pipeline_root: Path) -> PipelinePlan:
    return pipeline_plan_from_dict(read_json(pipeline_root / "pipeline_plan.json"))


def is_stage_batch_root(path: Path) -> bool:
    manifest = path / "manifest.json"
    return manifest.exists() and read_json(manifest).get("kind") == "stage_batch"


def is_pipeline_root(path: Path) -> bool:
    manifest = path / "manifest.json"
    return manifest.exists() and read_json(manifest).get("kind") == "pipeline"


def iter_stage_run_dirs(root: Path) -> Iterable[Path]:
    if is_stage_batch_root(root):
        yield from sorted((root / "runs").glob("*"))
        return
    if is_pipeline_root(root):
        for stage_root in sorted((root / "stage_batches").glob("*")):
            runs_dir = stage_root / "runs"
            if runs_dir.exists():
                yield from sorted(runs_dir.glob("*"))
        return
    raise FileNotFoundError(f"Not a stage batch or pipeline root: {root}")


def collect_stage_statuses(root: Path) -> list[StageStatusRecord]:
    statuses: list[StageStatusRecord] = []
    for run_dir in iter_stage_run_dirs(root):
        status = read_stage_status(run_dir)
        if status is not None:
            statuses.append(status)
    return statuses


def load_stage_outputs(run_dir: Path) -> dict[str, Any] | None:
    """Read raw outputs payload from disk, validating its schema.

    Returns the raw dict so callers can keep using key-based access. The
    schema check is enforced via ``stage_outputs_record_from_dict``; if the
    payload is malformed this raises rather than returning a half-validated
    dict.
    """
    path = stage_outputs_path(run_dir)
    if not path.exists():
        return None
    payload = read_json(path)
    stage_outputs_record_from_dict(payload)  # validate schema; raises on mismatch
    return payload


def run_definitions_from_stage_batch(batch: StageBatchPlan) -> tuple[RunDefinition, ...]:
    runs: list[RunDefinition] = []
    seen: set[str] = set()
    for instance in sorted(batch.stage_instances, key=lambda item: item.run_index):
        if instance.run_id in seen:
            continue
        seen.add(instance.run_id)
        runs.append(
            RunDefinition(
                run_id=instance.run_id,
                run_index=instance.run_index,
                matrix_assignments=dict(instance.matrix_assignments),
                spec_snapshot_digest=instance.spec_snapshot_digest,
            )
        )
    return tuple(runs)


def plan_for_run_dir(run_dir: Path) -> StageInstancePlan | None:
    path = stage_plan_path(run_dir)
    if not path.exists():
        return None
    return stage_instance_plan_from_dict(read_json(path))
