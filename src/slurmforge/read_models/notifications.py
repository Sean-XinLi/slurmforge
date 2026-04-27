from __future__ import annotations

from pathlib import Path
from typing import Any

from ..notifications.models import NotificationRunStatusInput, NotificationStageStatusInput, NotificationSummaryInput
from ..storage.loader import (
    is_stage_batch_root,
    is_train_eval_pipeline_root,
    load_execution_stage_batch_plan,
    load_train_eval_pipeline_plan,
)
from .status import refresh_stage_batch_status, refresh_train_eval_pipeline_status


def notification_plan_for_root(root: Path) -> Any:
    target = Path(root)
    if is_train_eval_pipeline_root(target):
        return load_train_eval_pipeline_plan(target).notification_plan
    if is_stage_batch_root(target):
        return load_execution_stage_batch_plan(target).notification_plan
    raise FileNotFoundError(f"not a stage batch or train/eval pipeline root: {target}")


def load_notification_summary_input(root: Path, *, event: str) -> NotificationSummaryInput:
    target = Path(root)
    if is_train_eval_pipeline_root(target):
        return _train_eval_pipeline_summary_input(target, event=event)
    if is_stage_batch_root(target):
        return _stage_batch_summary_input(target, event=event)
    raise FileNotFoundError(f"not a stage batch or train/eval pipeline root: {target}")


def _train_eval_pipeline_summary_input(root: Path, *, event: str) -> NotificationSummaryInput:
    plan = load_train_eval_pipeline_plan(root)
    snapshot = refresh_train_eval_pipeline_status(root)
    first_batch = next(iter(plan.stage_batches.values()))
    return NotificationSummaryInput(
        event=event,
        root_kind="train_eval_pipeline",
        root=str(root),
        project=first_batch.project,
        experiment=first_batch.experiment,
        object_id=plan.pipeline_id,
        state="missing" if snapshot.pipeline_status is None else snapshot.pipeline_status.state,
        run_statuses=_run_status_inputs(snapshot),
        stage_statuses=_stage_status_inputs(snapshot),
    )


def _stage_batch_summary_input(root: Path, *, event: str) -> NotificationSummaryInput:
    batch = load_execution_stage_batch_plan(root)
    snapshot = refresh_stage_batch_status(root)
    return NotificationSummaryInput(
        event=event,
        root_kind="stage_batch",
        root=str(root),
        project=batch.project,
        experiment=batch.experiment,
        object_id=batch.batch_id,
        state=_root_state(snapshot),
        run_statuses=_run_status_inputs(snapshot),
        stage_statuses=_stage_status_inputs(snapshot),
    )


def _run_status_inputs(snapshot) -> tuple[NotificationRunStatusInput, ...]:
    return tuple(NotificationRunStatusInput(run_id=status.run_id, state=status.state) for status in snapshot.run_statuses)


def _stage_status_inputs(snapshot) -> tuple[NotificationStageStatusInput, ...]:
    return tuple(
        NotificationStageStatusInput(
            run_id=status.run_id,
            stage_name=status.stage_name,
            state=status.state,
            failure_class=status.failure_class or "",
            reason=status.reason,
        )
        for status in snapshot.stage_statuses
    )


def _root_state(snapshot) -> str:
    states = [status.state for status in snapshot.run_statuses]
    if not states:
        return "missing"
    if any(state == "failed" for state in states):
        return "failed"
    if any(state == "blocked" for state in states):
        return "blocked"
    if all(state == "success" for state in states):
        return "success"
    if any(state == "running" for state in states):
        return "running"
    if any(state == "queued" for state in states):
        return "queued"
    return "planned"
