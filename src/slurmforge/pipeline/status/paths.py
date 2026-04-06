from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def job_key_from_env(env: Mapping[str, str] | None = None) -> str:
    values = env or os.environ
    slurm_job_id = (values.get("SLURM_JOB_ID") or "").strip()
    if slurm_job_id:
        return slurm_job_id
    array_job_id = (values.get("SLURM_ARRAY_JOB_ID") or "na").strip() or "na"
    array_task_id = (values.get("SLURM_ARRAY_TASK_ID") or "na").strip() or "na"
    return f"{array_job_id}_{array_task_id}"


def result_dir_for_run(run_dir: Path, env: Mapping[str, str] | None = None) -> Path:
    return run_dir / f"job-{job_key_from_env(env)}"


def latest_result_dir_pointer_path_for_run(run_dir: Path) -> Path:
    return run_dir / "meta" / "latest_result_dir.json"


def status_path_for_result_dir(result_dir: Path) -> Path:
    return result_dir / "meta" / "execution_status.json"


def attempt_result_path_for_result_dir(result_dir: Path) -> Path:
    return result_dir / "meta" / "attempt_result.json"
