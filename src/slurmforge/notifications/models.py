from __future__ import annotations

from dataclasses import dataclass, field

from ..contracts import (
    NotificationRunStatusInput as NotificationRunStatusInput,
    NotificationStageStatusInput as NotificationStageStatusInput,
    NotificationSummaryInput as NotificationSummaryInput,
)
from ..io import SchemaVersion


@dataclass(frozen=True)
class FailedStageSummary:
    run_id: str
    stage_name: str
    state: str
    failure_class: str = ""
    reason: str = ""


@dataclass(frozen=True)
class NotificationSummary:
    event: str
    root_kind: str
    root: str
    project: str
    experiment: str
    object_id: str
    state: str
    total_runs: int
    run_counts: dict[str, int] = field(default_factory=dict)
    stage_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    failed_stages: tuple[FailedStageSummary, ...] = ()
    schema_version: int = SchemaVersion.NOTIFICATION


@dataclass(frozen=True)
class NotificationDeliveryRecord:
    event: str
    root_kind: str
    root: str
    backend: str
    state: str
    recipients: tuple[str, ...] = ()
    subject: str = ""
    sent_at: str = ""
    reason: str = ""
    scheduler_job_id: str = ""
    sbatch_path: str = ""
    barrier_job_ids: tuple[str, ...] = ()
    dependency_job_ids: tuple[str, ...] = ()
    submitted_at: str = ""
    schema_version: int = SchemaVersion.NOTIFICATION
