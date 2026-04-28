from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion, write_json
from ..storage.loader import load_train_eval_pipeline_plan
from .aggregation import aggregate_run_statuses, aggregate_train_eval_pipeline_status
from .detection import detect_root
from .models import RootStatusSnapshot
from .runs import collect_stage_statuses


def refresh_stage_batch_status(batch_root: Path) -> RootStatusSnapshot:
    root = Path(batch_root).resolve()
    statuses = collect_stage_statuses(root)
    snapshot = RootStatusSnapshot(
        root=root,
        kind="stage_batch",
        stage_statuses=tuple(statuses),
        run_statuses=aggregate_run_statuses(statuses),
    )
    write_json(root / "run_status.json", {"schema_version": SchemaVersion.STATUS, "runs": snapshot.run_statuses})
    return snapshot


def refresh_train_eval_pipeline_status(pipeline_root: Path) -> RootStatusSnapshot:
    root = Path(pipeline_root).resolve()
    plan = load_train_eval_pipeline_plan(root)
    statuses = collect_stage_statuses(root)
    pipeline_status = aggregate_train_eval_pipeline_status(plan.pipeline_id, statuses)
    snapshot = RootStatusSnapshot(
        root=root,
        kind="train_eval_pipeline",
        stage_statuses=tuple(statuses),
        run_statuses=aggregate_run_statuses(statuses),
        pipeline_status=pipeline_status,
    )
    write_json(root / "run_status.json", {"schema_version": SchemaVersion.STATUS, "runs": snapshot.run_statuses})
    write_json(root / "train_eval_pipeline_status.json", pipeline_status)
    return snapshot


def refresh_root_status(root: Path) -> RootStatusSnapshot:
    descriptor = detect_root(root)
    if descriptor.kind == "stage_batch":
        return refresh_stage_batch_status(descriptor.root)
    return refresh_train_eval_pipeline_status(descriptor.root)


def load_root_status_snapshot(root: Path) -> RootStatusSnapshot:
    descriptor = detect_root(root)
    statuses = collect_stage_statuses(descriptor.root)
    run_statuses = aggregate_run_statuses(statuses)
    if descriptor.kind == "stage_batch":
        return RootStatusSnapshot(
            root=descriptor.root,
            kind="stage_batch",
            stage_statuses=tuple(statuses),
            run_statuses=run_statuses,
        )
    plan = load_train_eval_pipeline_plan(descriptor.root)
    return RootStatusSnapshot(
        root=descriptor.root,
        kind="train_eval_pipeline",
        stage_statuses=tuple(statuses),
        run_statuses=run_statuses,
        pipeline_status=aggregate_train_eval_pipeline_status(plan.pipeline_id, statuses),
    )
