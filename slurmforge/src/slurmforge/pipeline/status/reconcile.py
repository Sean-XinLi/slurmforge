from __future__ import annotations

from pathlib import Path

from .classifier import classify_logs_only
from .lifecycle import finalize_execution_status, now_iso, strip_job_prefix
from .models import ExecutionStatus
from .paths import attempt_result_path_for_result_dir, status_path_for_result_dir
from .slurm import SlurmJobState, query_slurm_job_state
from .store import read_attempt_result, read_execution_status, read_latest_result_dir, write_execution_status


def _latest_job_result_dir(run_dir: Path) -> Path | None:
    explicit = read_latest_result_dir(run_dir)
    if explicit is not None and explicit.is_dir():
        return explicit
    return None


def _build_live_status(
    *,
    latest_result_dir: Path,
    status: ExecutionStatus,
    slurm_state: SlurmJobState,
) -> ExecutionStatus:
    return ExecutionStatus(
        state=slurm_state.state,
        slurm_state=slurm_state.raw_state,
        failure_class=None,
        failed_stage=None,
        reason=slurm_state.reason,
        shell_exit_code=status.shell_exit_code,
        job_key=status.job_key or strip_job_prefix(latest_result_dir.name),
        slurm_job_id=status.slurm_job_id,
        slurm_array_job_id=status.slurm_array_job_id,
        slurm_array_task_id=status.slurm_array_task_id,
        started_at=status.started_at,
        finished_at="",
        result_dir=str(latest_result_dir),
        train_log=status.train_log,
        eval_log=status.eval_log,
        slurm_out=status.slurm_out,
        slurm_err=status.slurm_err,
    )


def _build_terminal_slurm_status(
    *,
    latest_result_dir: Path,
    status: ExecutionStatus,
    slurm_state: SlurmJobState,
) -> ExecutionStatus:
    return ExecutionStatus(
        state=slurm_state.state,
        slurm_state=slurm_state.raw_state,
        failure_class=slurm_state.failure_class,
        failed_stage="executor" if slurm_state.state == "failed" else None,
        reason=slurm_state.reason,
        shell_exit_code=1 if slurm_state.state == "failed" else 0,
        job_key=status.job_key or strip_job_prefix(latest_result_dir.name),
        slurm_job_id=status.slurm_job_id,
        slurm_array_job_id=status.slurm_array_job_id,
        slurm_array_task_id=status.slurm_array_task_id,
        started_at=status.started_at,
        finished_at=now_iso(),
        result_dir=str(latest_result_dir),
        train_log=status.train_log,
        eval_log=status.eval_log,
        slurm_out=status.slurm_out,
        slurm_err=status.slurm_err,
    )


def _build_failed_status_from_logs(
    *,
    latest_result_dir: Path,
    status: ExecutionStatus | None,
    failure_class: str,
    failed_stage: str,
    reason: str,
) -> ExecutionStatus:
    return ExecutionStatus(
        state="failed",
        slurm_state=status.slurm_state if status is not None else "",
        failure_class=failure_class,
        failed_stage=failed_stage,
        reason=reason,
        shell_exit_code=1,
        job_key=(status.job_key if status is not None and status.job_key else strip_job_prefix(latest_result_dir.name)),
        slurm_job_id=status.slurm_job_id if status is not None else "",
        slurm_array_job_id=status.slurm_array_job_id if status is not None else "",
        slurm_array_task_id=status.slurm_array_task_id if status is not None else "",
        started_at=status.started_at if status is not None else "",
        finished_at=now_iso(),
        result_dir=str(latest_result_dir),
        train_log=status.train_log if status is not None else None,
        eval_log=status.eval_log if status is not None else None,
        slurm_out=status.slurm_out if status is not None else None,
        slurm_err=status.slurm_err if status is not None else None,
    )


def _build_missing_metadata_status(latest_result_dir: Path) -> ExecutionStatus:
    return ExecutionStatus(
        state="failed",
        slurm_state="",
        failure_class="executor_error",
        failed_stage="executor",
        reason="missing execution_status.json and attempt_result.json",
        result_dir=str(latest_result_dir),
        job_key=strip_job_prefix(latest_result_dir.name),
    )


def load_or_infer_execution_status(run_dir: Path) -> ExecutionStatus | None:
    latest_result_dir = _latest_job_result_dir(run_dir)
    if latest_result_dir is None:
        return None

    status_path = status_path_for_result_dir(latest_result_dir)
    status = read_execution_status(status_path)
    attempt = read_attempt_result(attempt_result_path_for_result_dir(latest_result_dir))

    if status is not None and status.state not in {"running", "pending"}:
        return status

    if status is not None and status.slurm_job_id:
        slurm_state = query_slurm_job_state(status.slurm_job_id)
        if slurm_state is not None:
            if slurm_state.state in {"pending", "running"}:
                live = _build_live_status(
                    latest_result_dir=latest_result_dir,
                    status=status,
                    slurm_state=slurm_state,
                )
                write_execution_status(status_path, live)
                return live
            if attempt is None:
                inferred = _build_terminal_slurm_status(
                    latest_result_dir=latest_result_dir,
                    status=status,
                    slurm_state=slurm_state,
                )
                write_execution_status(status_path, inferred)
                return inferred

    if attempt is not None:
        shell_exit_code = 0 if attempt.train_exit_code in {None, 0} and attempt.eval_exit_code in {None, 0} else 1
        started_at = status.started_at if status is not None else ""
        return finalize_execution_status(
            result_dir=latest_result_dir,
            started_at=started_at,
            shell_exit_code=shell_exit_code,
            status_hint=status,
        )

    inferred_from_logs = classify_logs_only(
        latest_result_dir,
        slurm_job_id=status.slurm_job_id if status is not None else None,
    )
    if inferred_from_logs is not None:
        failure_class, failed_stage, reason = inferred_from_logs
        inferred = _build_failed_status_from_logs(
            latest_result_dir=latest_result_dir,
            status=status,
            failure_class=failure_class,
            failed_stage=failed_stage,
            reason=reason,
        )
        write_execution_status(status_path, inferred)
        return inferred

    if status is not None:
        return status

    inferred = _build_missing_metadata_status(latest_result_dir)
    write_execution_status(status_path, inferred)
    return inferred


def status_matches_query(status: ExecutionStatus | None, query: str) -> bool:
    normalized = (query or "failed").strip().lower()
    if normalized == "all":
        return True
    if normalized in {"failed", "non_success", "nonsuccess"}:
        return status is None or status.state != "success"
    if normalized == "success":
        return status is not None and status.state == "success"
    if normalized == "pending":
        return status is not None and status.state == "pending"
    if normalized == "running":
        return status is not None and status.state == "running"
    return status is not None and status.failure_class == normalized
