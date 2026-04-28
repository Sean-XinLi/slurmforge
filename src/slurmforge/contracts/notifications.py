from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NotificationRunStatusInput:
    run_id: str
    state: str


@dataclass(frozen=True)
class NotificationStageStatusInput:
    run_id: str
    stage_name: str
    state: str
    failure_class: str = ""
    reason: str = ""


@dataclass(frozen=True)
class NotificationSummaryInput:
    event: str
    root_kind: str
    root: str
    project: str
    experiment: str
    object_id: str
    state: str
    run_statuses: tuple[NotificationRunStatusInput, ...] = ()
    stage_statuses: tuple[NotificationStageStatusInput, ...] = ()
