from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Mapping

from .models import AttemptResult
from .paths import job_key_from_env


def complete_attempt_result(attempt: AttemptResult, *, result_dir: Path) -> AttemptResult:
    resolved_result_dir = result_dir.resolve()
    log_dir = resolved_result_dir / "logs"
    return replace(
        attempt,
        result_dir=attempt.result_dir or str(resolved_result_dir),
        log_dir=attempt.log_dir or str(log_dir),
    )


def build_attempt_result(
    *,
    result_dir: Path,
    train_exit_code: int,
    eval_exit_code: int,
    env: Mapping[str, str] | None = None,
) -> AttemptResult:
    values = env or os.environ
    resolved_result_dir = result_dir.resolve()
    log_dir = resolved_result_dir / "logs"
    slurm_job_id = str(values.get("SLURM_JOB_ID", ""))
    return AttemptResult(
        train_exit_code=int(train_exit_code),
        eval_exit_code=int(eval_exit_code),
        job_key=job_key_from_env(values),
        slurm_job_id=slurm_job_id,
        slurm_array_job_id=str(values.get("SLURM_ARRAY_JOB_ID", "")),
        slurm_array_task_id=str(values.get("SLURM_ARRAY_TASK_ID", "")),
        result_dir=str(resolved_result_dir),
        log_dir=str(log_dir),
        train_log=str(log_dir / "train.log"),
        eval_log=str(log_dir / "eval.log"),
        slurm_out=str(log_dir / f"slurm-{slurm_job_id}.out") if slurm_job_id else None,
        slurm_err=str(log_dir / f"slurm-{slurm_job_id}.err") if slurm_job_id else None,
    )
