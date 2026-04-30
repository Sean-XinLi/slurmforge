from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..control_paths import control_submissions_path
from ..errors import ConfigContractError, RecordContractError
from ..io import SchemaVersion, read_json, require_schema, to_jsonable, utc_now, write_json

CONTROL_KIND_STAGE_INSTANCE_GATE = "stage_instance_gate"
CONTROL_KIND_DISPATCH_CATCHUP_GATE = "dispatch_catchup_gate"
CONTROL_KIND_TERMINAL_NOTIFICATION = "terminal_notification"

CONTROL_STATE_SUBMITTING = "submitting"
CONTROL_STATE_SUBMITTED = "submitted"
CONTROL_STATE_UNCERTAIN = "uncertain"
CONTROL_STATE_FAILED = "failed"

CONTROL_ON_ERROR_RAISE_UNCERTAIN = "raise_uncertain"
CONTROL_ON_ERROR_RECORD_FAILED = "record_failed"


@dataclass(frozen=True)
class ControlSubmitResult:
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    state: str = CONTROL_STATE_SUBMITTED
    reason: str = ""


@dataclass
class ControlSubmissionRecord:
    key: str
    kind: str
    target_kind: str
    target_id: str
    state: str
    sbatch_paths: tuple[str, ...] = ()
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    dependency_job_ids: tuple[str, ...] = ()
    reason: str = ""
    started_at: str = ""
    submitted_at: str = ""
    failed_at: str = ""


@dataclass
class ControlSubmissionLedger:
    schema_version: int
    updated_at: str
    submissions: dict[str, ControlSubmissionRecord] = field(default_factory=dict)


def control_submission_key(kind: str, *, target_id: str) -> str:
    if not target_id:
        raise ConfigContractError(f"Control submission `{kind}` requires target_id")
    return f"{kind}:{target_id}"


def empty_control_submission_ledger() -> ControlSubmissionLedger:
    return ControlSubmissionLedger(
        schema_version=SchemaVersion.CONTROL_SUBMISSIONS,
        updated_at=utc_now(),
    )


def read_control_submission_ledger(pipeline_root: Path) -> ControlSubmissionLedger:
    path = control_submissions_path(pipeline_root)
    if not path.exists():
        return empty_control_submission_ledger()
    payload = read_json(path)
    require_schema(
        payload,
        name="control_submissions",
        version=SchemaVersion.CONTROL_SUBMISSIONS,
    )
    submissions_payload = payload.get("submissions")
    if not isinstance(submissions_payload, dict):
        raise RecordContractError("control_submissions.submissions must be an object")
    submissions: dict[str, ControlSubmissionRecord] = {}
    for key, record in submissions_payload.items():
        if not isinstance(record, dict):
            raise RecordContractError(
                f"control_submissions.submissions.{key} must be an object"
            )
        submissions[str(key)] = control_submission_from_payload(str(key), dict(record))
    return ControlSubmissionLedger(
        schema_version=SchemaVersion.CONTROL_SUBMISSIONS,
        updated_at=str(payload.get("updated_at") or ""),
        submissions=submissions,
    )


def write_control_submission_ledger(
    pipeline_root: Path, ledger: ControlSubmissionLedger
) -> None:
    ledger.schema_version = SchemaVersion.CONTROL_SUBMISSIONS
    ledger.updated_at = utc_now()
    write_json(control_submissions_path(pipeline_root), ledger)


def submitted_control_records(pipeline_root: Path) -> dict[str, dict[str, Any]]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {
        key: to_jsonable(record)
        for key, record in ledger.submissions.items()
        if record.state == CONTROL_STATE_SUBMITTED
    }


def submitted_control_job_ids(pipeline_root: Path) -> dict[str, tuple[str, ...]]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {
        key: record.scheduler_job_ids
        for key, record in ledger.submissions.items()
        if record.state == CONTROL_STATE_SUBMITTED and record.scheduler_job_ids
    }


def control_submission_from_payload(
    key: str, payload: dict[str, Any]
) -> ControlSubmissionRecord:
    return ControlSubmissionRecord(
        key=key,
        kind=str(payload["kind"]),
        target_kind=str(payload.get("target_kind") or ""),
        target_id=str(payload.get("target_id") or ""),
        state=str(payload["state"]),
        sbatch_paths=tuple(str(item) for item in payload.get("sbatch_paths") or ()),
        scheduler_job_ids=tuple(
            str(item) for item in payload.get("scheduler_job_ids") or ()
        ),
        barrier_job_ids=tuple(str(item) for item in payload.get("barrier_job_ids") or ()),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids") or ()
        ),
        reason=str(payload.get("reason") or ""),
        started_at=str(payload.get("started_at") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
        failed_at=str(payload.get("failed_at") or ""),
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
        write_control_submission_ledger(pipeline_root, ledger)
        if on_error == CONTROL_ON_ERROR_RECORD_FAILED:
            return record
        raise

    record.state = result.state
    record.scheduler_job_ids = result.scheduler_job_ids
    record.barrier_job_ids = result.barrier_job_ids
    record.reason = result.reason
    if result.state == CONTROL_STATE_SUBMITTED:
        record.submitted_at = utc_now()
    elif result.state in {CONTROL_STATE_FAILED, CONTROL_STATE_UNCERTAIN}:
        record.failed_at = utc_now()
    write_control_submission_ledger(pipeline_root, ledger)
    return record
