from __future__ import annotations

from dataclasses import dataclass, field

from ..control_job_contract import (
    CONTROL_STATE_SUBMITTED,
    ControlJobRecord,
)
from ..io import SchemaVersion, utc_now

CONTROL_ON_ERROR_RAISE_UNCERTAIN = "raise_uncertain"
CONTROL_ON_ERROR_RECORD_FAILED = "record_failed"

CONTROL_ON_ERROR_POLICIES = (
    CONTROL_ON_ERROR_RAISE_UNCERTAIN,
    CONTROL_ON_ERROR_RECORD_FAILED,
)


@dataclass(frozen=True)
class ControlSubmitResult:
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    state: str = CONTROL_STATE_SUBMITTED
    reason: str = ""

@dataclass
class ControlSubmissionLedger:
    schema_version: int
    updated_at: str
    submissions: dict[str, ControlJobRecord] = field(default_factory=dict)


def empty_control_submission_ledger() -> ControlSubmissionLedger:
    return ControlSubmissionLedger(
        schema_version=SchemaVersion.CONTROL_SUBMISSIONS,
        updated_at=utc_now(),
    )
