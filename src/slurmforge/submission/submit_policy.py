from __future__ import annotations

from ..errors import ConfigContractError
from .ledger_records import GROUP_JOB_STATES, GROUP_STATE_SUBMITTING
from .models import GroupSubmissionRecord

SUBMIT_POLICY_NEW_ONLY = "new_only"
SUBMIT_POLICY_RECOVER_PARTIAL = "recover_partial"
SUBMIT_POLICIES = (SUBMIT_POLICY_NEW_ONLY, SUBMIT_POLICY_RECOVER_PARTIAL)


def validate_submit_policy(policy: str) -> None:
    if policy not in SUBMIT_POLICIES:
        raise ConfigContractError(f"Unsupported submission policy: {policy}")


def adopt_existing_group_job(
    *,
    stage_name: str,
    group_id: str,
    record: GroupSubmissionRecord,
    policy: str,
    group_job_ids: dict[str, str],
) -> bool:
    if not record.scheduler_job_id or record.state not in GROUP_JOB_STATES:
        return False
    if policy == SUBMIT_POLICY_NEW_ONLY:
        raise ConfigContractError(
            f"Stage batch `{stage_name}` already has submitted group `{group_id}` "
            f"with scheduler job `{record.scheduler_job_id}`; submit a derived batch for a new execution"
        )
    group_job_ids[group_id] = record.scheduler_job_id
    return True


def require_retryable_group(record: GroupSubmissionRecord, *, stage_name: str) -> None:
    if record.state == GROUP_STATE_SUBMITTING and not record.scheduler_job_id:
        raise ConfigContractError(
            f"Submission ledger for `{stage_name}` is uncertain at group `{record.group_id}`; "
            "manual reconcile is required before retrying"
        )
