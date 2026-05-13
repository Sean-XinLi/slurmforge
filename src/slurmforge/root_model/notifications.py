from __future__ import annotations

from pathlib import Path

from ..contracts import (
    NotificationRunStatusInput,
    NotificationStageStatusInput,
    NotificationSummaryInput,
)
from ..plans.notifications import NotificationPlan
from ..storage.plan_reader import (
    load_execution_stage_batch_plan,
    load_train_eval_pipeline_plan,
)
from .aggregation import root_state
from .detection import detect_root
from .models import (
    RootNotificationSnapshot,
    RootStatusSnapshot,
)
from .snapshots import load_root_status_snapshot


def notification_plan_for_root(root: Path) -> NotificationPlan:
    descriptor = detect_root(root)
    if descriptor.kind == "train_eval_pipeline":
        return load_train_eval_pipeline_plan(descriptor.root).notification_plan
    return load_execution_stage_batch_plan(descriptor.root).notification_plan


def load_root_notification_snapshot(
    root: Path, *, event: str
) -> RootNotificationSnapshot:
    status = load_root_status_snapshot(root)
    notification_plan = notification_plan_for_root(status.root)
    return RootNotificationSnapshot(
        root=status.root,
        kind=status.kind,
        notification_plan=notification_plan,
        summary_input=_summary_input(status, event=event),
        status=status,
    )


def load_notification_summary_input(
    root: Path, *, event: str
) -> NotificationSummaryInput:
    return load_root_notification_snapshot(root, event=event).summary_input


def _summary_input(
    status: RootStatusSnapshot, *, event: str
) -> NotificationSummaryInput:
    if status.kind == "train_eval_pipeline":
        return _train_eval_pipeline_summary_input(status, event=event)
    return _stage_batch_summary_input(status, event=event)


def _train_eval_pipeline_summary_input(
    status: RootStatusSnapshot, *, event: str
) -> NotificationSummaryInput:
    plan = load_train_eval_pipeline_plan(status.root)
    first_batch = next(iter(plan.stage_batches.values()))
    pipeline_status = status.pipeline_status
    return NotificationSummaryInput(
        event=event,
        root_kind="train_eval_pipeline",
        root=str(status.root),
        project=first_batch.project,
        experiment=first_batch.experiment,
        object_id=plan.pipeline_id,
        state="missing" if pipeline_status is None else pipeline_status.state,
        run_statuses=_run_status_inputs(status),
        stage_statuses=_stage_status_inputs(status),
    )


def _stage_batch_summary_input(
    status: RootStatusSnapshot, *, event: str
) -> NotificationSummaryInput:
    batch = load_execution_stage_batch_plan(status.root)
    return NotificationSummaryInput(
        event=event,
        root_kind="stage_batch",
        root=str(status.root),
        project=batch.project,
        experiment=batch.experiment,
        object_id=batch.batch_id,
        state=root_state(status.run_statuses),
        run_statuses=_run_status_inputs(status),
        stage_statuses=_stage_status_inputs(status),
    )


def _run_status_inputs(
    status: RootStatusSnapshot,
) -> tuple[NotificationRunStatusInput, ...]:
    return tuple(
        NotificationRunStatusInput(run_id=item.run_id, state=item.state)
        for item in status.run_statuses
    )


def _stage_status_inputs(
    status: RootStatusSnapshot,
) -> tuple[NotificationStageStatusInput, ...]:
    return tuple(
        NotificationStageStatusInput(
            run_id=item.run_id,
            stage_name=item.stage_name,
            state=item.state,
            failure_class=item.failure_class or "",
            reason=item.reason,
        )
        for item in status.stage_statuses
    )
