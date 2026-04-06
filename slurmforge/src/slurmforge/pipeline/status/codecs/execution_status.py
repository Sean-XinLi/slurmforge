from __future__ import annotations

from pathlib import Path
from typing import Any

from ..models import ExecutionStatus
from .path_fields import (
    path_relative_to_base,
    reject_absolute_internal_fields,
    require_result_dir_context,
    require_result_dir_rel,
    resolve_internal_result_path,
)


def serialize_execution_status(status: ExecutionStatus) -> dict[str, Any]:
    return {
        "schema_version": int(status.schema_version),
        "state": str(status.state),
        "slurm_state": str(status.slurm_state),
        "failure_class": None if status.failure_class is None else str(status.failure_class),
        "failed_stage": None if status.failed_stage is None else str(status.failed_stage),
        "reason": str(status.reason),
        "train_exit_code": None if status.train_exit_code is None else int(status.train_exit_code),
        "eval_exit_code": None if status.eval_exit_code is None else int(status.eval_exit_code),
        "shell_exit_code": None if status.shell_exit_code is None else int(status.shell_exit_code),
        "job_key": str(status.job_key),
        "slurm_job_id": str(status.slurm_job_id),
        "slurm_array_job_id": str(status.slurm_array_job_id),
        "slurm_array_task_id": str(status.slurm_array_task_id),
        "started_at": str(status.started_at),
        "finished_at": str(status.finished_at),
        "result_dir_rel": "." if status.result_dir else None,
        "train_log_rel": path_relative_to_base(status.result_dir, status.train_log),
        "eval_log_rel": path_relative_to_base(status.result_dir, status.eval_log),
        "slurm_out_rel": path_relative_to_base(status.result_dir, status.slurm_out),
        "slurm_err_rel": path_relative_to_base(status.result_dir, status.slurm_err),
    }


def deserialize_execution_status(payload: dict[str, Any], *, result_dir: Path | None = None) -> ExecutionStatus:
    if not isinstance(payload, dict):
        raise TypeError("execution status must be a mapping")
    require_result_dir_rel(payload, payload_name="execution status")
    reject_absolute_internal_fields(
        payload,
        payload_name="execution status",
        field_names=("result_dir", "train_log", "eval_log", "slurm_out", "slurm_err"),
    )
    current_result_dir = require_result_dir_context(result_dir, payload_name="execution status")
    return ExecutionStatus(
        schema_version=int(payload.get("schema_version", 1) or 1),
        state=str(payload.get("state", "running")),
        slurm_state=str(payload.get("slurm_state", "")),
        failure_class=None if payload.get("failure_class") in {None, ""} else str(payload.get("failure_class")),
        failed_stage=None if payload.get("failed_stage") in {None, ""} else str(payload.get("failed_stage")),
        reason=str(payload.get("reason", "")),
        train_exit_code=None if payload.get("train_exit_code") is None else int(payload.get("train_exit_code")),
        eval_exit_code=None if payload.get("eval_exit_code") is None else int(payload.get("eval_exit_code")),
        shell_exit_code=None if payload.get("shell_exit_code") is None else int(payload.get("shell_exit_code")),
        job_key=str(payload.get("job_key", "")),
        slurm_job_id=str(payload.get("slurm_job_id", "")),
        slurm_array_job_id=str(payload.get("slurm_array_job_id", "")),
        slurm_array_task_id=str(payload.get("slurm_array_task_id", "")),
        started_at=str(payload.get("started_at", "")),
        finished_at=str(payload.get("finished_at", "")),
        result_dir=str(current_result_dir),
        train_log=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("train_log_rel") in {None, ""} else str(payload.get("train_log_rel")),
            field_name="execution status.train_log_rel",
        ),
        eval_log=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("eval_log_rel") in {None, ""} else str(payload.get("eval_log_rel")),
            field_name="execution status.eval_log_rel",
        ),
        slurm_out=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("slurm_out_rel") in {None, ""} else str(payload.get("slurm_out_rel")),
            field_name="execution status.slurm_out_rel",
        ),
        slurm_err=resolve_internal_result_path(
            current_result_dir=current_result_dir,
            rel_value=None if payload.get("slurm_err_rel") in {None, ""} else str(payload.get("slurm_err_rel")),
            field_name="execution status.slurm_err_rel",
        ),
    )
