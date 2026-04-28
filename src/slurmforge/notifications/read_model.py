from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion
from ..storage.loader import (
    collect_stage_statuses,
    is_stage_batch_root,
    is_train_eval_pipeline_root,
    load_execution_stage_batch_plan,
    load_train_eval_pipeline_plan,
)
from ..status.models import RunStatusRecord, StageStatusRecord, TERMINAL_STATES
from .models import NotificationRunStatusInput, NotificationStageStatusInput, NotificationSummaryInput


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
    statuses = collect_stage_statuses(root)
    first_batch = next(iter(plan.stage_batches.values()))
    return NotificationSummaryInput(
        event=event,
        root_kind="train_eval_pipeline",
        root=str(root),
        project=first_batch.project,
        experiment=first_batch.experiment,
        object_id=plan.pipeline_id,
        state=_pipeline_state(statuses),
        run_statuses=_run_status_inputs(_run_statuses(statuses)),
        stage_statuses=_stage_status_inputs(statuses),
    )


def _stage_batch_summary_input(root: Path, *, event: str) -> NotificationSummaryInput:
    batch = load_execution_stage_batch_plan(root)
    statuses = collect_stage_statuses(root)
    run_statuses = _run_statuses(statuses)
    return NotificationSummaryInput(
        event=event,
        root_kind="stage_batch",
        root=str(root),
        project=batch.project,
        experiment=batch.experiment,
        object_id=batch.batch_id,
        state=_root_state(run_statuses),
        run_statuses=_run_status_inputs(run_statuses),
        stage_statuses=_stage_status_inputs(statuses),
    )


def _run_statuses(statuses: list[StageStatusRecord]) -> tuple[RunStatusRecord, ...]:
    grouped: dict[str, list[StageStatusRecord]] = {}
    for status in statuses:
        grouped.setdefault(status.run_id, []).append(status)
    return tuple(_aggregate_run_status(run_id, grouped[run_id]) for run_id in sorted(grouped))


def _aggregate_run_status(run_id: str, statuses: list[StageStatusRecord]) -> RunStatusRecord:
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
    return RunStatusRecord(schema_version=SchemaVersion.STATUS, run_id=run_id, state=state, stage_states=stage_states)


def _run_status_inputs(run_statuses: tuple[RunStatusRecord, ...]) -> tuple[NotificationRunStatusInput, ...]:
    return tuple(NotificationRunStatusInput(run_id=status.run_id, state=status.state) for status in run_statuses)


def _stage_status_inputs(statuses: list[StageStatusRecord]) -> tuple[NotificationStageStatusInput, ...]:
    return tuple(
        NotificationStageStatusInput(
            run_id=status.run_id,
            stage_name=status.stage_name,
            state=status.state,
            failure_class=status.failure_class or "",
            reason=status.reason,
        )
        for status in statuses
    )


def _root_state(run_statuses: tuple[RunStatusRecord, ...]) -> str:
    states = [status.state for status in run_statuses]
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


def _pipeline_state(statuses: list[StageStatusRecord]) -> str:
    if not statuses:
        return "missing"
    if any(status.state == "failed" for status in statuses):
        return "failed"
    if any(status.state == "blocked" for status in statuses):
        return "blocked"
    if all(status.state in TERMINAL_STATES for status in statuses) and all(
        status.state == "success" for status in statuses
    ):
        return "success"
    if any(status.state == "running" for status in statuses):
        return "running"
    if any(status.state == "queued" for status in statuses):
        return "queued"
    return "planned"
