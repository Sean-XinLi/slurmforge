from __future__ import annotations

from pathlib import Path

from ..config_contract.option_sets import EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
from ..control_paths import control_submissions_path
from ..emit.pipeline_notification import (
    write_pipeline_notification_barrier_file,
    write_pipeline_notification_submit_file,
)
from ..errors import ConfigContractError
from ..io import utc_now
from ..notifications.models import NotificationSubmissionRecord
from ..notifications.policy import email_notification_enabled
from ..notifications.records import read_notification_record
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..submission.notification_mail import submit_slurm_mail_notification
from .control_submissions import (
    CONTROL_KIND_TERMINAL_NOTIFICATION,
    CONTROL_STATE_FAILED,
    CONTROL_STATE_SUBMITTING,
    CONTROL_STATE_SUBMITTED,
    CONTROL_STATE_UNCERTAIN,
    control_submission_key,
    read_control_submissions,
    write_control_submissions,
)


def submit_pipeline_terminal_notification(
    pipeline_root: Path,
    plan,
    *,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> NotificationSubmissionRecord | None:
    event = EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
    if not email_notification_enabled(plan.notification_plan, event):
        return None
    key = control_submission_key(
        CONTROL_KIND_TERMINAL_NOTIFICATION,
        target_id=event,
    )
    ledger = read_control_submissions(pipeline_root)
    submissions = ledger.setdefault("submissions", {})
    existing = submissions.get(key)
    if isinstance(existing, dict) and existing.get("state") == CONTROL_STATE_SUBMITTED:
        record = read_notification_record(pipeline_root, event)
        if record is not None:
            return record
    if isinstance(existing, dict) and existing.get("state") == CONTROL_STATE_SUBMITTING:
        existing["state"] = CONTROL_STATE_UNCERTAIN
        existing["reason"] = (
            "previous terminal notification reached scheduler call without recorded job ids"
        )
        write_control_submissions(pipeline_root, ledger)
        raise ConfigContractError(
            f"Terminal notification submission is uncertain for `{key}`; inspect "
            f"{control_submissions_path(pipeline_root)} before retrying"
        )
    notification_path = write_pipeline_notification_submit_file(plan, event=event)
    submissions[key] = {
        "state": CONTROL_STATE_SUBMITTING,
        "kind": CONTROL_KIND_TERMINAL_NOTIFICATION,
        "target_kind": "workflow",
        "target_id": event,
        "sbatch_paths": [str(notification_path)],
        "dependency_job_ids": list(dependency_job_ids),
        "started_at": utc_now(),
    }
    write_control_submissions(pipeline_root, ledger)
    notification = submit_slurm_mail_notification(
        root=pipeline_root,
        root_kind="train_eval_pipeline",
        event=event,
        notification_plan=plan.notification_plan,
        dependency_job_ids=dependency_job_ids,
        sbatch_path=notification_path,
        client=client,
        barrier_path_factory=lambda barrier_index: write_pipeline_notification_barrier_file(
            plan,
            event=event,
            barrier_index=barrier_index,
        ),
        max_dependency_length=max_dependency_length,
    )
    if notification is None:
        return None
    submissions[key].update(
        {
            "state": (
                CONTROL_STATE_SUBMITTED
                if notification.state == "submitted"
                else CONTROL_STATE_FAILED
            ),
            "scheduler_job_ids": list(notification.scheduler_job_ids),
            "barrier_job_ids": list(notification.barrier_job_ids),
            "reason": notification.reason,
            "submitted_at": notification.submitted_at,
        }
    )
    write_control_submissions(pipeline_root, ledger)
    return notification
