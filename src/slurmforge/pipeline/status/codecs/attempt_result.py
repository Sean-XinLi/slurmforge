from __future__ import annotations

from pathlib import Path
from typing import Any

from ..models import AttemptResult
from .path_fields import (
    path_relative_to_base,
    reject_absolute_internal_fields,
    require_result_dir_context,
    require_result_dir_rel,
    resolve_internal_result_path,
)


def serialize_attempt_result(attempt: AttemptResult) -> dict[str, Any]:
    return {
        "train_exit_code": None if attempt.train_exit_code is None else int(attempt.train_exit_code),
        "eval_exit_code": None if attempt.eval_exit_code is None else int(attempt.eval_exit_code),
        "job_key": str(attempt.job_key),
        "slurm_job_id": str(attempt.slurm_job_id),
        "slurm_array_job_id": str(attempt.slurm_array_job_id),
        "slurm_array_task_id": str(attempt.slurm_array_task_id),
        "result_dir_rel": "." if attempt.result_dir else None,
        "log_dir_rel": path_relative_to_base(attempt.result_dir, attempt.log_dir),
        "train_log_rel": path_relative_to_base(attempt.result_dir, attempt.train_log),
        "eval_log_rel": path_relative_to_base(attempt.result_dir, attempt.eval_log),
        "slurm_out_rel": path_relative_to_base(attempt.result_dir, attempt.slurm_out),
        "slurm_err_rel": path_relative_to_base(attempt.result_dir, attempt.slurm_err),
    }


def deserialize_attempt_result(payload: dict[str, Any], *, result_dir: Path | None = None) -> AttemptResult:
    if not isinstance(payload, dict):
        raise TypeError("attempt result must be a mapping")
    require_result_dir_rel(payload, payload_name="attempt result")
    reject_absolute_internal_fields(
        payload,
        payload_name="attempt result",
        field_names=("result_dir", "log_dir", "train_log", "eval_log", "slurm_out", "slurm_err"),
    )
    current_result_dir = require_result_dir_context(result_dir, payload_name="attempt result")
    return AttemptResult(
        train_exit_code=None if payload.get("train_exit_code") is None else int(payload.get("train_exit_code")),
        eval_exit_code=None if payload.get("eval_exit_code") is None else int(payload.get("eval_exit_code")),
        job_key=str(payload.get("job_key", "")),
        slurm_job_id=str(payload.get("slurm_job_id", "")),
        slurm_array_job_id=str(payload.get("slurm_array_job_id", "")),
        slurm_array_task_id=str(payload.get("slurm_array_task_id", "")),
        result_dir=str(current_result_dir),
        log_dir=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("log_dir_rel") in {None, ""} else str(payload.get("log_dir_rel")),
            field_name="attempt result.log_dir_rel",
        )
        or "",
        train_log=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("train_log_rel") in {None, ""} else str(payload.get("train_log_rel")),
            field_name="attempt result.train_log_rel",
        ),
        eval_log=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("eval_log_rel") in {None, ""} else str(payload.get("eval_log_rel")),
            field_name="attempt result.eval_log_rel",
        ),
        slurm_out=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("slurm_out_rel") in {None, ""} else str(payload.get("slurm_out_rel")),
            field_name="attempt result.slurm_out_rel",
        ),
        slurm_err=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("slurm_err_rel") in {None, ""} else str(payload.get("slurm_err_rel")),
            field_name="attempt result.slurm_err_rel",
        ),
    )
