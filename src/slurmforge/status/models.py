from __future__ import annotations

from dataclasses import dataclass, field

from ..io import SchemaVersion


TERMINAL_STATES = {"success", "failed", "cancelled", "skipped", "blocked"}


@dataclass(frozen=True)
class StageAttemptRecord:
    attempt_id: str
    stage_instance_id: str
    attempt_source: str = "executor"
    attempt_state: str = "starting"
    scheduler_job_id: str = ""
    scheduler_array_job_id: str = ""
    scheduler_array_task_id: str = ""
    scheduler_state: str = ""
    scheduler_exit_code: str = ""
    node_list: str = ""
    started_by_executor: bool = True
    executor_started_at: str = ""
    executor_finished_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    exit_code: int | None = None
    failure_class: str | None = None
    reason: str = ""
    log_paths: tuple[str, ...] = ()
    artifact_paths: tuple[str, ...] = ()
    artifact_manifest_path: str = ""
    schema_version: int = SchemaVersion.STATUS


@dataclass(frozen=True)
class StageStatusRecord:
    schema_version: int
    stage_instance_id: str
    run_id: str
    stage_name: str
    state: str = "planned"
    latest_attempt_id: str | None = None
    latest_output_digest: str | None = None
    failure_class: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class RunStatusRecord:
    schema_version: int
    run_id: str
    state: str
    stage_states: dict[str, str] = field(default_factory=dict)
    reason: str = ""


@dataclass(frozen=True)
class TrainEvalPipelineStatusRecord:
    schema_version: int
    pipeline_id: str
    state: str
    total_runs: int
    stage_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    reason: str = ""
