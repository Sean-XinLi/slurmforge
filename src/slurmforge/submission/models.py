from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PreparedSubmission:
    batch_root: Path
    batch_id: str
    stage_name: str
    generation_id: str
    manifest_path: Path
    ledger_path: Path
    materialization_state: str = "ready"


@dataclass(frozen=True)
class SubmissionGroupState:
    group_id: str
    sbatch_path: str
    dependency: str | None
    scheduler_job_id: str | None
    state: str
    submitted_at: str | None
    reason: str


@dataclass(frozen=True)
class SubmissionState:
    batch_root: Path
    batch_id: str
    stage_name: str
    generation_id: str
    ledger_state: str
    materialization_state: str
    submitted_group_job_ids: dict[str, str]
    groups: tuple[SubmissionGroupState, ...] = ()


@dataclass(frozen=True)
class SubmitGeneration:
    generation_id: str
    batch_id: str
    stage_name: str
    manifest_path: str
    submit_script: str
    sbatch_paths_by_group: dict[str, str]
    dependency_plan: tuple[dict[str, object], ...] = ()


@dataclass
class GroupSubmissionRecord:
    group_id: str
    sbatch_path: str
    dependency: str | None = None
    scheduler_job_id: str | None = None
    state: str = "planned"
    submitted_at: str | None = None
    reason: str = ""


@dataclass
class SubmissionLedger:
    schema_version: int
    batch_id: str
    stage_name: str
    generation_id: str
    state: str
    groups: dict[str, GroupSubmissionRecord] = field(default_factory=dict)
