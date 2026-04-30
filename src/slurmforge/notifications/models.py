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
class NotificationSubmissionRecord:
    event: str
    root_kind: str
    root: str
    backend: str
    state: str
    recipients: tuple[str, ...] = ()
    scheduler_job_ids: tuple[str, ...] = ()
    sbatch_paths: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    dependency_job_ids: tuple[str, ...] = ()
    dependency_type: str = ""
    mail_type: str = ""
    submitted_at: str = ""
    reason: str = ""
    schema_version: int = SchemaVersion.NOTIFICATION
