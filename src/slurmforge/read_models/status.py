from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, write_json
from ..plans import TRAIN_EVAL_PIPELINE_KIND
from ..status.models import RunStatusRecord, StageStatusRecord, TERMINAL_STATES, TrainEvalPipelineStatusRecord
from ..storage.loader import (
    collect_stage_statuses,
    is_stage_batch_root,
    is_train_eval_pipeline_root,
    load_train_eval_pipeline_plan,
)


@dataclass(frozen=True)
class RootStatusSnapshot:
    root: Path
    kind: str
    stage_statuses: tuple[StageStatusRecord, ...]
    run_statuses: tuple[RunStatusRecord, ...]
    pipeline_status: TrainEvalPipelineStatusRecord | None = None
    schema_version: int = SchemaVersion.STATUS


def aggregate_run_status(run_id: str, statuses: list[StageStatusRecord]) -> RunStatusRecord:
    stage_states = {status.stage_name: status.state for status in statuses}
    if not statuses:
        state = "missing"
    elif any(status.state == "failed" for status in statuses):
        state = "failed"
    elif any(status.state == "blocked" for status in statuses):
        state = "blocked"
    elif all(status.state == "success" for status in statuses):
        state = "success"
    elif any(status.state == "running" for status in statuses):
        state = "running"
    elif any(status.state == "queued" for status in statuses):
        state = "queued"
    else:
        state = "planned"
    return RunStatusRecord(
        schema_version=SchemaVersion.STATUS,
        run_id=run_id,
        state=state,
        stage_states=stage_states,
    )


def aggregate_train_eval_pipeline_status(
    pipeline_id: str, statuses: list[StageStatusRecord]
) -> TrainEvalPipelineStatusRecord:
    run_ids = sorted({status.run_id for status in statuses})
    stage_counts: dict[str, dict[str, int]] = {}
    for status in statuses:
        counts = stage_counts.setdefault(status.stage_name, {})
        counts[status.state] = counts.get(status.state, 0) + 1
    if not statuses:
        state = "missing"
    elif any(status.state == "failed" for status in statuses):
        state = "failed"
    elif any(status.state == "blocked" for status in statuses):
        state = "blocked"
    elif all(status.state in TERMINAL_STATES for status in statuses) and all(
        status.state == "success" for status in statuses
    ):
        state = "success"
    elif any(status.state == "running" for status in statuses):
        state = "running"
    elif any(status.state == "queued" for status in statuses):
        state = "queued"
    else:
        state = "planned"
    return TrainEvalPipelineStatusRecord(
        schema_version=SchemaVersion.STATUS,
        pipeline_id=pipeline_id,
        state=state,
        total_runs=len(run_ids),
        stage_counts=stage_counts,
    )


def _run_statuses(statuses: list[StageStatusRecord]) -> tuple[RunStatusRecord, ...]:
    grouped: dict[str, list[StageStatusRecord]] = {}
    for status in statuses:
        grouped.setdefault(status.run_id, []).append(status)
    return tuple(aggregate_run_status(run_id, grouped[run_id]) for run_id in sorted(grouped))


def refresh_stage_batch_status(batch_root: Path) -> RootStatusSnapshot:
    root = Path(batch_root)
    statuses = collect_stage_statuses(root)
    snapshot = RootStatusSnapshot(
        root=root,
        kind="stage_batch",
        stage_statuses=tuple(statuses),
        run_statuses=_run_statuses(statuses),
    )
    write_json(root / "run_status.json", {"schema_version": SchemaVersion.STATUS, "runs": snapshot.run_statuses})
    return snapshot


def refresh_train_eval_pipeline_status(pipeline_root: Path) -> RootStatusSnapshot:
    root = Path(pipeline_root)
    plan = load_train_eval_pipeline_plan(root)
    statuses = collect_stage_statuses(root)
    pipeline_status = aggregate_train_eval_pipeline_status(plan.pipeline_id, statuses)
    snapshot = RootStatusSnapshot(
        root=root,
        kind=TRAIN_EVAL_PIPELINE_KIND,
        stage_statuses=tuple(statuses),
        run_statuses=_run_statuses(statuses),
        pipeline_status=pipeline_status,
    )
    write_json(root / "run_status.json", {"schema_version": SchemaVersion.STATUS, "runs": snapshot.run_statuses})
    write_json(root / "train_eval_pipeline_status.json", pipeline_status)
    return snapshot


def refresh_root_status(root: Path) -> RootStatusSnapshot:
    target = Path(root)
    if is_train_eval_pipeline_root(target):
        return refresh_train_eval_pipeline_status(target)
    if is_stage_batch_root(target):
        return refresh_stage_batch_status(target)
    raise FileNotFoundError(f"not a stage batch or train/eval pipeline root: {target}")
