from __future__ import annotations

from pathlib import Path
from typing import Callable

from ..control_paths import control_submissions_path
from ..errors import ConfigContractError
from ..io import utc_now
from .control_submission_ledger import (
    read_control_submission_ledger,
    write_control_submission_ledger,
)
from .control_submission_records import (
    CONTROL_ON_ERROR_POLICIES,
    CONTROL_ON_ERROR_RAISE_UNCERTAIN,
    CONTROL_ON_ERROR_RECORD_FAILED,
    CONTROL_STATE_FAILED,
    CONTROL_STATE_SUBMITTED,
    CONTROL_STATE_SUBMITTING,
    CONTROL_STATE_UNCERTAIN,
    ControlSubmissionRecord,
    ControlSubmitResult,
    validate_control_record,
)


def submit_control_once(
    pipeline_root: Path,
    *,
    key: str,
    kind: str,
    target_kind: str,
    target_id: str,
    sbatch_paths: tuple[Path, ...],
    dependency_job_ids: tuple[str, ...],
    submitter: Callable[[], ControlSubmitResult],
    on_error: str = CONTROL_ON_ERROR_RAISE_UNCERTAIN,
) -> ControlSubmissionRecord:
    _validate_control_submit_request(
        key=key,
        kind=kind,
        target_kind=target_kind,
        target_id=target_id,
        sbatch_paths=sbatch_paths,
        on_error=on_error,
    )
    ledger = read_control_submission_ledger(pipeline_root)
    existing = ledger.submissions.get(key)
    if existing is not None:
        if existing.state == CONTROL_STATE_SUBMITTED and existing.scheduler_job_ids:
            return existing
        if existing.state in {CONTROL_STATE_SUBMITTING, CONTROL_STATE_UNCERTAIN}:
            existing.state = CONTROL_STATE_UNCERTAIN
            existing.reason = (
                existing.reason
                or "previous submission reached scheduler call without recorded job ids"
            )
            write_control_submission_ledger(pipeline_root, ledger)
            raise ConfigContractError(
                f"Control submission is uncertain for `{key}`; inspect "
                f"{control_submissions_path(pipeline_root)} before retrying"
            )

    record = ControlSubmissionRecord(
        key=key,
        state=CONTROL_STATE_SUBMITTING,
        kind=kind,
        target_kind=target_kind,
        target_id=target_id,
        sbatch_paths=tuple(str(path) for path in sbatch_paths),
        dependency_job_ids=dependency_job_ids,
        started_at=utc_now(),
    )
    ledger.submissions[key] = record
    write_control_submission_ledger(pipeline_root, ledger)

    try:
        result = submitter()
    except Exception as exc:
        record.state = (
            CONTROL_STATE_FAILED
            if on_error == CONTROL_ON_ERROR_RECORD_FAILED
            else CONTROL_STATE_UNCERTAIN
        )
        record.reason = str(exc)
        record.failed_at = utc_now()
        validate_control_record(
            key=record.key,
            kind=record.kind,
            target_kind=record.target_kind,
            target_id=record.target_id,
            state=record.state,
            sbatch_paths=record.sbatch_paths,
            scheduler_job_ids=record.scheduler_job_ids,
            reason=record.reason,
        )
        write_control_submission_ledger(pipeline_root, ledger)
        if on_error == CONTROL_ON_ERROR_RECORD_FAILED:
            return record
        raise

    record.state = result.state
    record.scheduler_job_ids = result.scheduler_job_ids
    record.barrier_job_ids = result.barrier_job_ids
    record.reason = result.reason
    validate_control_record(
        key=record.key,
        kind=record.kind,
        target_kind=record.target_kind,
        target_id=record.target_id,
        state=record.state,
        sbatch_paths=record.sbatch_paths,
        scheduler_job_ids=record.scheduler_job_ids,
        reason=record.reason,
    )
    if result.state == CONTROL_STATE_SUBMITTED:
        record.submitted_at = utc_now()
    elif result.state in {CONTROL_STATE_FAILED, CONTROL_STATE_UNCERTAIN}:
        record.failed_at = utc_now()
    write_control_submission_ledger(pipeline_root, ledger)
    return record


def _validate_control_submit_request(
    *,
    key: str,
    kind: str,
    target_kind: str,
    target_id: str,
    sbatch_paths: tuple[Path, ...],
    on_error: str,
) -> None:
    if on_error not in CONTROL_ON_ERROR_POLICIES:
        raise ConfigContractError(f"Unsupported control submission on_error: {on_error}")
    validate_control_record(
        key=key,
        kind=kind,
        target_kind=target_kind,
        target_id=target_id,
        state=CONTROL_STATE_SUBMITTING,
        sbatch_paths=tuple(str(path) for path in sbatch_paths),
        scheduler_job_ids=(),
        reason="",
    )
