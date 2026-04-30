from __future__ import annotations

from pathlib import Path

from ..config_contract.option_sets import EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
from ..emit.pipeline_notification import (
    write_pipeline_notification_barrier_file,
    write_pipeline_notification_submit_file,
)
from ..notifications.models import NotificationSubmissionRecord
from ..notifications.policy import email_notification_enabled
from ..notifications.records import (
    append_notification_event,
    read_notification_record,
    write_notification_record,
)
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..submission.notification_mail import SLURM_MAIL_BACKEND, submit_slurm_mail_jobs
from .control_submission_records import (
    CONTROL_KIND_TERMINAL_NOTIFICATION,
    CONTROL_ON_ERROR_RECORD_FAILED,
    ControlSubmissionRecord,
    ControlSubmitResult,
    control_submission_key,
)
from .control_submission_submit import (
    submit_control_once,
)


def terminal_notification_control_key() -> str:
    return control_submission_key(
        CONTROL_KIND_TERMINAL_NOTIFICATION,
        target_id=EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED,
    )


def submit_pipeline_terminal_notification(
    pipeline_root: Path,
    plan,
    *,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> ControlSubmissionRecord | None:
    event = EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
    if not email_notification_enabled(plan.notification_plan, event):
        return None
    notification_path = write_pipeline_notification_submit_file(plan, event=event)
    key = terminal_notification_control_key()
    record = submit_control_once(
        pipeline_root,
        key=key,
        kind=CONTROL_KIND_TERMINAL_NOTIFICATION,
        target_kind="workflow",
        target_id=event,
        sbatch_paths=(notification_path,),
        dependency_job_ids=dependency_job_ids,
        on_error=CONTROL_ON_ERROR_RECORD_FAILED,
        submitter=lambda: _submit_terminal_mail_jobs(
            pipeline_root,
            plan,
            dependency_job_ids=dependency_job_ids,
            notification_path=notification_path,
            client=client,
            max_dependency_length=max_dependency_length,
        ),
    )
    _write_terminal_notification_record(pipeline_root, plan, record)
    return record


def _submit_terminal_mail_jobs(
    pipeline_root: Path,
    plan,
    *,
    dependency_job_ids: tuple[str, ...],
    notification_path: Path,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> ControlSubmitResult:
    event = EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
    result = submit_slurm_mail_jobs(
        root=pipeline_root,
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
    return ControlSubmitResult(
        state=result.state,
        scheduler_job_ids=result.scheduler_job_ids,
        barrier_job_ids=result.barrier_job_ids,
        reason=result.reason,
    )


def _write_terminal_notification_record(
    pipeline_root: Path,
    plan,
    record: ControlSubmissionRecord,
) -> NotificationSubmissionRecord:
    event = EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
    existing = read_notification_record(pipeline_root, event, SLURM_MAIL_BACKEND)
    if existing is not None and existing.state == record.state:
        return existing
    email = plan.notification_plan.email
    notification = NotificationSubmissionRecord(
        event=event,
        root_kind="train_eval_pipeline",
        root=str(pipeline_root),
        backend=SLURM_MAIL_BACKEND,
        state=record.state,
        recipients=tuple(str(item) for item in email.recipients),
        scheduler_job_ids=record.scheduler_job_ids,
        sbatch_paths=record.sbatch_paths,
        barrier_job_ids=record.barrier_job_ids,
        dependency_job_ids=record.dependency_job_ids,
        dependency_type=email.when,
        mail_type=email.mail_type,
        submitted_at=record.submitted_at,
        reason=record.reason,
    )
    write_notification_record(pipeline_root, notification)
    append_notification_event(
        pipeline_root,
        (
            "notification_submitted"
            if record.state == "submitted"
            else "notification_submit_failed"
        ),
        notification_event=event,
        backend=SLURM_MAIL_BACKEND,
        scheduler_job_ids=record.scheduler_job_ids,
        barrier_job_ids=record.barrier_job_ids,
        dependency_job_ids=record.dependency_job_ids,
        recipients=tuple(str(item) for item in email.recipients),
        reason=record.reason,
    )
    return notification
