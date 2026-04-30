from __future__ import annotations

from .ledger_records import (
    LEDGER_STATE_FAILED,
    LEDGER_STATE_PARTIAL,
    LEDGER_STATE_SUBMITTED,
    LEDGER_STATE_SUBMITTING,
    LEDGER_STATE_UNCERTAIN,
    GROUP_STATE_FAILED,
    GROUP_STATE_SUBMITTED,
    GROUP_STATE_SUBMITTING,
)
from .models import GroupSubmissionRecord, SubmissionLedger


def mark_group_uncertain(ledger: SubmissionLedger, record: GroupSubmissionRecord) -> None:
    ledger.state = LEDGER_STATE_UNCERTAIN
    record.reason = "group may have reached sbatch without a recorded scheduler job id"


def mark_group_submitting(
    ledger: SubmissionLedger,
    record: GroupSubmissionRecord,
    *,
    dependency: str | None,
    submitted_group_count: int,
) -> None:
    record.state = GROUP_STATE_SUBMITTING
    record.dependency = dependency
    record.scheduler_job_id = None
    record.submitted_at = None
    record.reason = ""
    ledger.state = LEDGER_STATE_PARTIAL if submitted_group_count else LEDGER_STATE_SUBMITTING


def mark_group_failed(
    ledger: SubmissionLedger,
    record: GroupSubmissionRecord,
    *,
    reason: str,
) -> None:
    record.state = GROUP_STATE_FAILED
    record.reason = reason
    ledger.state = LEDGER_STATE_FAILED


def mark_group_submitted(
    ledger: SubmissionLedger,
    record: GroupSubmissionRecord,
    *,
    job_id: str,
    submitted_at: str,
) -> None:
    record.scheduler_job_id = job_id
    record.submitted_at = submitted_at
    record.state = GROUP_STATE_SUBMITTED
    record.reason = ""
    ledger.state = LEDGER_STATE_PARTIAL


def mark_batch_submitted(ledger: SubmissionLedger) -> None:
    ledger.state = LEDGER_STATE_SUBMITTED
