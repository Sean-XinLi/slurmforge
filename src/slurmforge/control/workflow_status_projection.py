from __future__ import annotations

from pathlib import Path

from ..storage.workflow_status_records import WorkflowStatusControlJobRecord
from .control_submission_ledger import read_control_submission_ledger
from .control_submission_records import ControlSubmissionRecord, validate_control_record


def workflow_status_control_jobs(
    pipeline_root: Path,
) -> dict[str, WorkflowStatusControlJobRecord]:
    ledger = read_control_submission_ledger(pipeline_root)
    return {
        key: control_submission_to_workflow_status_job(record)
        for key, record in ledger.submissions.items()
    }


def control_submission_to_workflow_status_job(
    record: ControlSubmissionRecord,
) -> WorkflowStatusControlJobRecord:
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
    return WorkflowStatusControlJobRecord(
        key=record.key,
        kind=record.kind,
        target_kind=record.target_kind,
        target_id=record.target_id,
        state=record.state,
        sbatch_paths=record.sbatch_paths,
        scheduler_job_ids=record.scheduler_job_ids,
        barrier_job_ids=record.barrier_job_ids,
        dependency_job_ids=record.dependency_job_ids,
        reason=record.reason,
        started_at=record.started_at,
        submitted_at=record.submitted_at,
        failed_at=record.failed_at,
    )
