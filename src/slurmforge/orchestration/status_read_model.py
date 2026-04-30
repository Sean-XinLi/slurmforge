from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..root_model.detection import detect_root
from ..root_model.runs import collect_stage_statuses
from ..root_model.snapshots import refresh_root_status
from ..status.query import state_matches
from ..storage.batch_materialization_records import read_materialization_status
from ..storage.runtime_batches import iter_runtime_batch_roots
from ..storage.workflow import read_workflow_status
from ..storage.workflow_status_records import WorkflowStatusRecord
from ..submission.reconcile import reconcile_root_submissions


@dataclass(frozen=True)
class StatusMaterializationView:
    stage_name: str
    state: str
    failure_class: str
    reason: str


@dataclass(frozen=True)
class StatusReadModel:
    root: Path
    root_kind: str
    query: str
    workflow_status: WorkflowStatusRecord | None
    materializations: tuple[StatusMaterializationView, ...]
    statuses: tuple[Any, ...]
    matched_statuses: tuple[Any, ...]
    counts: dict[str, int]
    stage_counts: dict[str, dict[str, int]]


def build_status_read_model(
    *,
    root: Path,
    status_query: str = "all",
    stage: str | None = None,
    reconcile: bool = False,
    missing_output_grace_seconds: int = 300,
) -> StatusReadModel:
    descriptor = detect_root(root)
    resolved_root = descriptor.root
    if reconcile:
        reconcile_root_submissions(
            resolved_root,
            stage=stage,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        refresh_root_status(resolved_root)

    materializations = _materialization_views(
        root=resolved_root,
        root_kind=descriptor.kind,
        stage=stage,
    )
    workflow_status = (
        read_workflow_status(resolved_root)
        if descriptor.kind == "train_eval_pipeline"
        else None
    )
    statuses = tuple(collect_stage_statuses(resolved_root))
    if stage:
        statuses = tuple(item for item in statuses if item.stage_name == stage)
    matched = tuple(item for item in statuses if state_matches(item, status_query))
    counts, stage_counts = _status_counts(statuses)
    return StatusReadModel(
        root=resolved_root,
        root_kind=descriptor.kind,
        query=status_query,
        workflow_status=workflow_status,
        materializations=materializations,
        statuses=statuses,
        matched_statuses=matched,
        counts=counts,
        stage_counts=stage_counts,
    )


def _materialization_views(
    *,
    root: Path,
    root_kind: str,
    stage: str | None,
) -> tuple[StatusMaterializationView, ...]:
    if root_kind == "stage_batch":
        materialization = read_materialization_status(root)
        return () if materialization is None else (_view(materialization),)
    if root_kind != "train_eval_pipeline":
        return ()
    views: list[StatusMaterializationView] = []
    for stage_root in iter_runtime_batch_roots(root):
        materialization = read_materialization_status(stage_root)
        if materialization is None:
            continue
        if stage is not None and materialization.stage_name != stage:
            continue
        views.append(_view(materialization))
    return tuple(views)


def _view(materialization) -> StatusMaterializationView:
    return StatusMaterializationView(
        stage_name=materialization.stage_name,
        state=materialization.state,
        failure_class=materialization.failure_class or "-",
        reason=materialization.reason,
    )


def _status_counts(statuses: tuple[Any, ...]) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    counts: dict[str, int] = {}
    stage_counts: dict[str, dict[str, int]] = {}
    for status in statuses:
        counts[status.state] = counts.get(status.state, 0) + 1
        by_stage = stage_counts.setdefault(status.stage_name, {})
        by_stage[status.state] = by_stage.get(status.state, 0) + 1
    return counts, stage_counts
