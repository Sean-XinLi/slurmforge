"""Canonical path resolution for execution journal files.

All execution file paths are derived here — backends and lifecycle import
from this module instead of ``pipeline.status.paths`` / ``pipeline.checkpoints.store``
/ ``pipeline.train_outputs.paths``.
"""
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


def latest_result_dir_pointer_path(run_dir: Path) -> Path:
    return run_dir / "meta" / "latest_result_dir.json"


def execution_status_path(result_dir: Path) -> Path:
    return result_dir / "meta" / "execution_status.json"


def attempt_result_path(result_dir: Path) -> Path:
    return result_dir / "meta" / "attempt_result.json"


def checkpoint_state_path(result_dir: Path) -> Path:
    return result_dir / "meta" / "checkpoint_state.json"


def train_outputs_manifest_path(result_dir: Path) -> Path:
    return result_dir / "meta" / "train_outputs.json"


def artifact_manifest_path(result_dir: Path) -> Path:
    return result_dir / "meta" / "artifact_manifest.json"
