from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Callable

from ..control_paths import control_submissions_path
from ..control_job_contract import (
    CONTROL_STATE_FAILED,
    CONTROL_STATE_SUBMITTED,
    CONTROL_STATE_SUBMITTING,
    CONTROL_STATE_UNCERTAIN,
    ControlJobRecord,
    validate_control_job_record,
)
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
    ControlSubmitResult,
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
) -> ControlJobRecord:
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
            ledger.submissions[key] = replace(
                existing,
                state=CONTROL_STATE_UNCERTAIN,
                reason=(
                    existing.reason
                    or "previous submission reached scheduler call without recorded job ids"
                ),
            )
            write_control_submission_ledger(pipeline_root, ledger)
            raise ConfigContractError(
                f"Control submission is uncertain for `{key}`; inspect "
                f"{control_submissions_path(pipeline_root)} before retrying"
            )

    record = ControlJobRecord(
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
        record = replace(
            record,
            state=(
                CONTROL_STATE_FAILED
                if on_error == CONTROL_ON_ERROR_RECORD_FAILED
                else CONTROL_STATE_UNCERTAIN
            ),
            reason=str(exc),
            failed_at=utc_now(),
        )
        validate_control_job_record(record, label="control submission")
        ledger.submissions[key] = record
        write_control_submission_ledger(pipeline_root, ledger)
        if on_error == CONTROL_ON_ERROR_RECORD_FAILED:
            return record
        raise

    record = replace(
        record,
        state=result.state,
        scheduler_job_ids=result.scheduler_job_ids,
        barrier_job_ids=result.barrier_job_ids,
        reason=result.reason,
    )
    validate_control_job_record(record, label="control submission")
    if result.state == CONTROL_STATE_SUBMITTED:
        record = replace(record, submitted_at=utc_now())
    elif result.state in {CONTROL_STATE_FAILED, CONTROL_STATE_UNCERTAIN}:
        record = replace(record, failed_at=utc_now())
    ledger.submissions[key] = record
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
    validate_control_job_record(
        ControlJobRecord(
            key=key,
            kind=kind,
            target_kind=target_kind,
            target_id=target_id,
            state=CONTROL_STATE_SUBMITTING,
            sbatch_paths=tuple(str(path) for path in sbatch_paths),
        ),
        label="control submission",
    )
