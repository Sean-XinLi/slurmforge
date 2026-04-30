from __future__ import annotations

from pathlib import Path
from typing import Any

from ..control_paths import control_submissions_path
from ..errors import RecordContractError
from ..io import SchemaVersion, read_json, require_schema, to_jsonable, utc_now, write_json
from ..record_fields import required_object, required_string
from .control_submission_records import (
    CONTROL_STATE_SUBMITTED,
    ControlSubmissionLedger,
    ControlSubmissionRecord,
    control_submission_from_payload,
    empty_control_submission_ledger,
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
    submissions_payload = required_object(
        payload, "submissions", label="control_submissions"
    )
    submissions: dict[str, ControlSubmissionRecord] = {}
    for key, record in submissions_payload.items():
        if not isinstance(record, dict):
            raise RecordContractError(
                f"control_submissions.submissions.{key} must be an object"
            )
        submissions[str(key)] = control_submission_from_payload(str(key), dict(record))
    return ControlSubmissionLedger(
        schema_version=SchemaVersion.CONTROL_SUBMISSIONS,
        updated_at=required_string(payload, "updated_at", label="control_submissions"),
        submissions=submissions,
    )


def write_control_submission_ledger(
    pipeline_root: Path, ledger: ControlSubmissionLedger
) -> None:
    ledger.schema_version = SchemaVersion.CONTROL_SUBMISSIONS
    ledger.updated_at = utc_now()
    write_json(control_submissions_path(pipeline_root), ledger)


def all_control_records(pipeline_root: Path) -> dict[str, dict[str, Any]]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {key: to_jsonable(record) for key, record in ledger.submissions.items()}


def submitted_control_job_ids(pipeline_root: Path) -> dict[str, tuple[str, ...]]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {
        key: record.scheduler_job_ids
        for key, record in ledger.submissions.items()
        if record.state == CONTROL_STATE_SUBMITTED and record.scheduler_job_ids
    }
