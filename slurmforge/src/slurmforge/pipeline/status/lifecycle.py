from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .classifier import classify_failure
from .models import ExecutionStatus
from .paths import (
    attempt_result_path_for_result_dir,
    job_key_from_env,
    result_dir_for_run,
    status_path_for_result_dir,
)
from .store import read_attempt_result, write_execution_status, write_latest_result_dir


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def strip_job_prefix(name: str) -> str:
    if name.startswith("job-"):
        return name[4:]
    return name


def begin_execution_status(run_dir: Path, env: Mapping[str, str] | None = None) -> tuple[Path, ExecutionStatus]:
    result_dir = result_dir_for_run(run_dir, env)
    status_path = status_path_for_result_dir(result_dir)
    values = env or os.environ
    status = ExecutionStatus(
        state="running",
        slurm_state="RUNNING" if values.get("SLURM_JOB_ID") else "",
        job_key=job_key_from_env(values),
        slurm_job_id=str(values.get("SLURM_JOB_ID", "")),
        slurm_array_job_id=str(values.get("SLURM_ARRAY_JOB_ID", "")),
        slurm_array_task_id=str(values.get("SLURM_ARRAY_TASK_ID", "")),
        started_at=now_iso(),
        result_dir=str(result_dir),
    )
    write_execution_status(status_path, status)
    write_latest_result_dir(run_dir, result_dir)
    return result_dir, status


def fail_execution_status(
    *,
    result_dir: Path,
    started_at: str,
    reason: str,
    shell_exit_code: int | None = 1,
    failure_class: str = "executor_error",
    failed_stage: str = "executor",
    job_key: str = "",
    slurm_job_id: str = "",
    slurm_array_job_id: str = "",
    slurm_array_task_id: str = "",
) -> ExecutionStatus:
    status = ExecutionStatus(
        state="failed",
        slurm_state="FAILED",
        failure_class=failure_class,
        failed_stage=failed_stage,
        reason=reason,
        shell_exit_code=shell_exit_code,
        job_key=job_key or strip_job_prefix(result_dir.name),
        slurm_job_id=slurm_job_id,
        slurm_array_job_id=slurm_array_job_id,
        slurm_array_task_id=slurm_array_task_id,
        started_at=started_at,
        finished_at=now_iso(),
        result_dir=str(result_dir),
    )
    write_execution_status(status_path_for_result_dir(result_dir), status)
    return status


def finalize_execution_status(
    *,
    result_dir: Path,
    started_at: str,
    shell_exit_code: int,
    status_hint: ExecutionStatus | None = None,
) -> ExecutionStatus:
    attempt = read_attempt_result(attempt_result_path_for_result_dir(result_dir))
    train_exit = attempt.train_exit_code if attempt else None
    eval_exit = attempt.eval_exit_code if attempt else None
    if shell_exit_code == 0 and train_exit in {None, 0} and eval_exit in {None, 0}:
        state = "success"
        failure_class = None
        failed_stage = None
        reason = "train/eval completed successfully"
    else:
        state = "failed"
        failure_class, failed_stage, reason = classify_failure(
            shell_exit_code=shell_exit_code,
            attempt=attempt,
            result_dir=result_dir,
        )

    hint = status_hint
    resolved_job_key = (
        attempt.job_key
        if attempt is not None and attempt.job_key
        else (hint.job_key if hint is not None and hint.job_key else strip_job_prefix(result_dir.name))
    )
    resolved_slurm_state = "COMPLETED" if state == "success" else (hint.slurm_state if hint is not None else "")
    status = ExecutionStatus(
        state=state,
        slurm_state=resolved_slurm_state,
        failure_class=failure_class,
        failed_stage=failed_stage,
        reason=reason,
        train_exit_code=train_exit,
        eval_exit_code=eval_exit,
        shell_exit_code=shell_exit_code,
        job_key=resolved_job_key,
        slurm_job_id=(
            attempt.slurm_job_id
            if attempt is not None and attempt.slurm_job_id
            else (hint.slurm_job_id if hint is not None else "")
        ),
        slurm_array_job_id=(
            attempt.slurm_array_job_id
            if attempt is not None and attempt.slurm_array_job_id
            else (hint.slurm_array_job_id if hint is not None else "")
        ),
        slurm_array_task_id=(
            attempt.slurm_array_task_id
            if attempt is not None and attempt.slurm_array_task_id
            else (hint.slurm_array_task_id if hint is not None else "")
        ),
        started_at=started_at,
        finished_at=now_iso(),
        result_dir=str(result_dir),
        train_log=attempt.train_log if attempt is not None and attempt.train_log else (hint.train_log if hint is not None else None),
        eval_log=attempt.eval_log if attempt is not None and attempt.eval_log else (hint.eval_log if hint is not None else None),
        slurm_out=attempt.slurm_out if attempt is not None and attempt.slurm_out else (hint.slurm_out if hint is not None else None),
        slurm_err=attempt.slurm_err if attempt is not None and attempt.slurm_err else (hint.slurm_err if hint is not None else None),
    )
    write_execution_status(status_path_for_result_dir(result_dir), status)
    return status
