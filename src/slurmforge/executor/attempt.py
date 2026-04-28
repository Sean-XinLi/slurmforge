from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, content_digest, read_json, utc_now, write_json
from ..plans.stage import StageInstancePlan
from ..status import StageAttemptRecord, StageStatusRecord, commit_attempt, commit_stage_status


@dataclass(frozen=True)
class ExecutionAttempt:
    instance: StageInstancePlan
    run_dir: Path
    attempt_id: str
    attempt_dir: Path
    log_dir: Path
    stdout_path: Path
    stderr_path: Path
    started: str
    scheduler_job_id: str
    scheduler_array_job_id: str
    scheduler_array_task_id: str
    node_list: str


def _next_attempt_id(run_dir: Path) -> str:
    attempts = run_dir / "attempts"
    if not attempts.exists():
        return "0001"
    existing = [int(path.name) for path in attempts.iterdir() if path.is_dir() and path.name.isdigit()]
    return f"{(max(existing) + 1) if existing else 1:04d}"


def begin_attempt(run_dir: Path, instance: StageInstancePlan) -> ExecutionAttempt:
    attempt_id = _next_attempt_id(run_dir)
    attempt_dir = run_dir / "attempts" / attempt_id
    log_dir = attempt_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    attempt = ExecutionAttempt(
        instance=instance,
        run_dir=run_dir,
        attempt_id=attempt_id,
        attempt_dir=attempt_dir,
        log_dir=log_dir,
        stdout_path=log_dir / f"{instance.stage_name}.out",
        stderr_path=log_dir / f"{instance.stage_name}.err",
        started=started,
        scheduler_job_id=os.environ.get("SLURM_JOB_ID", ""),
        scheduler_array_job_id=os.environ.get("SLURM_ARRAY_JOB_ID", ""),
        scheduler_array_task_id=os.environ.get("SLURM_ARRAY_TASK_ID", ""),
        node_list=os.environ.get("SLURM_NODELIST", ""),
    )
    commit_attempt(run_dir, _running_attempt_record(attempt))
    write_json(attempt_dir / "launcher_plan.json", instance.launcher_plan)
    write_json(attempt_dir / "environment_plan.json", instance.environment_plan)
    write_json(attempt_dir / "before_steps.json", {"steps": instance.before_steps})
    commit_stage_status(
        run_dir,
        StageStatusRecord(
            schema_version=SchemaVersion.STATUS,
            stage_instance_id=instance.stage_instance_id,
            run_id=instance.run_id,
            stage_name=instance.stage_name,
            state="running",
            latest_attempt_id=attempt_id,
        ),
        allow_new_attempt=True,
        source="executor",
    )
    return attempt


def complete_attempt(
    attempt: ExecutionAttempt,
    *,
    exit_code: int | None,
    failure_class: str | None,
    reason: str,
    artifact_paths: tuple[str, ...],
) -> int:
    finished = utc_now()
    commit_attempt(
        attempt.run_dir,
        StageAttemptRecord(
            attempt_id=attempt.attempt_id,
            stage_instance_id=attempt.instance.stage_instance_id,
            attempt_source="executor",
            attempt_state="final",
            scheduler_job_id=attempt.scheduler_job_id,
            scheduler_array_job_id=attempt.scheduler_array_job_id,
            scheduler_array_task_id=attempt.scheduler_array_task_id,
            scheduler_state="",
            scheduler_exit_code="",
            node_list=attempt.node_list,
            started_by_executor=True,
            executor_started_at=attempt.started,
            executor_finished_at=finished,
            started_at=attempt.started,
            finished_at=finished,
            exit_code=exit_code,
            failure_class=failure_class,
            reason=reason,
            log_paths=(str(attempt.stdout_path), str(attempt.stderr_path)),
            artifact_paths=artifact_paths,
            artifact_manifest_path=_artifact_manifest_path(attempt),
        ),
    )
    status_state = "success" if exit_code == 0 and failure_class is None else "failed"
    commit_stage_status(
        attempt.run_dir,
        StageStatusRecord(
            schema_version=SchemaVersion.STATUS,
            stage_instance_id=attempt.instance.stage_instance_id,
            run_id=attempt.instance.run_id,
            stage_name=attempt.instance.stage_name,
            state=status_state,
            latest_attempt_id=attempt.attempt_id,
            latest_output_digest=_stage_output_digest(attempt.run_dir) if status_state == "success" else None,
            failure_class=failure_class,
            reason=reason,
        ),
        allow_new_attempt=True,
        source="executor",
    )
    return 0 if status_state == "success" else int(exit_code or 1)


def _running_attempt_record(attempt: ExecutionAttempt) -> StageAttemptRecord:
    return StageAttemptRecord(
        attempt_id=attempt.attempt_id,
        stage_instance_id=attempt.instance.stage_instance_id,
        attempt_source="executor",
        attempt_state="running",
        scheduler_job_id=attempt.scheduler_job_id,
        scheduler_array_job_id=attempt.scheduler_array_job_id,
        scheduler_array_task_id=attempt.scheduler_array_task_id,
        node_list=attempt.node_list,
        started_by_executor=True,
        executor_started_at=attempt.started,
        started_at=attempt.started,
        reason="executor started",
    )


def _artifact_manifest_path(attempt: ExecutionAttempt) -> str:
    path = attempt.attempt_dir / "artifacts" / "artifact_manifest.json"
    return str(path.resolve()) if path.exists() else ""


def _stage_output_digest(run_dir: Path) -> str | None:
    path = run_dir / "stage_outputs.json"
    return content_digest(read_json(path)) if path.exists() else None
