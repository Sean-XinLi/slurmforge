from __future__ import annotations

from pathlib import Path

from ..control_job_contract import ControlJobRecord, validate_control_job_record
from .control_submission_ledger import read_control_submission_ledger


def workflow_status_control_jobs(
    pipeline_root: Path,
) -> dict[str, ControlJobRecord]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {
        key: control_submission_to_workflow_status_job(record)
        for key, record in ledger.submissions.items()
    }


def control_submission_to_workflow_status_job(
    record: ControlJobRecord,
) -> ControlJobRecord:
    validate_control_job_record(record, label="workflow status control job")
    return record
