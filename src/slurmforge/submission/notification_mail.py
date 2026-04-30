from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..io import diagnostic_path, utc_now, write_exception_diagnostic
from ..notifications.models import NotificationSubmissionRecord
from ..notifications.policy import email_notification_enabled
from ..notifications.records import (
    append_notification_event,
    read_notification_record,
    write_notification_record,
)
from ..slurm import SlurmClientProtocol, SlurmSubmitOptions
from .dependency_tree import MAX_DEPENDENCY_LENGTH, submit_dependency_barriers


SLURM_MAIL_BACKEND = "slurm_mail"


@dataclass(frozen=True)
class MailJobSubmissionResult:
    state: str
    scheduler_job_ids: tuple[str, ...] = ()
    sbatch_paths: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    reason: str = ""
    submitted_at: str = ""


def submit_slurm_mail_jobs(
    *,
    root: Path,
    event: str,
    notification_plan,
    dependency_job_ids: tuple[str, ...],
    sbatch_path: Path,
    client: SlurmClientProtocol,
    barrier_path_factory: Callable[[int], Path],
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> MailJobSubmissionResult:
    target = Path(root)
    email = notification_plan.email
    dependency_type = email.when
    job_ids: list[str] = []
    sbatch_paths: list[str] = []
    barrier_job_ids: tuple[str, ...] = ()
    try:
        dependency = ""
        if dependency_job_ids:
            dependency, barrier_job_ids = submit_dependency_barriers(
                dependency_job_ids=dependency_job_ids,
                client=client,
                barrier_path_factory=barrier_path_factory,
                dependency_type=dependency_type,
                max_dependency_length=max_dependency_length,
            )
        for recipient in email.recipients:
            job_id = client.submit(
                sbatch_path,
                options=SlurmSubmitOptions(
                    dependency=dependency,
                    mail_user=str(recipient),
                    mail_type=email.mail_type,
                ),
            )
            job_ids.append(job_id)
            sbatch_paths.append(str(sbatch_path))
    except Exception as exc:
        write_exception_diagnostic(
            diagnostic_path(
                target,
                "notifications",
                f"{event}_slurm_mail_submit_traceback.log",
            ),
            exc,
        )
        return MailJobSubmissionResult(
            state="uncertain" if job_ids or barrier_job_ids else "failed",
            scheduler_job_ids=tuple(job_ids),
            sbatch_paths=tuple(sbatch_paths),
            barrier_job_ids=barrier_job_ids,
            reason=str(exc),
        )
    return MailJobSubmissionResult(
        state="submitted",
        scheduler_job_ids=tuple(job_ids),
        sbatch_paths=tuple(sbatch_paths),
        barrier_job_ids=barrier_job_ids,
        submitted_at=utc_now(),
    )


def submit_slurm_mail_notification(
    *,
    root: Path,
    root_kind: str,
    event: str,
    notification_plan,
    dependency_job_ids: tuple[str, ...],
    sbatch_path: Path,
    client: SlurmClientProtocol,
    barrier_path_factory: Callable[[int], Path],
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> NotificationSubmissionRecord | None:
    if not email_notification_enabled(notification_plan, event):
        return None
    target = Path(root)
    existing = read_notification_record(target, event, SLURM_MAIL_BACKEND)
    if existing is not None and existing.state == "submitted":
        return existing

    email = notification_plan.email
    dependency_type = email.when
    base = {
        "event": event,
        "root_kind": root_kind,
        "root": str(target),
        "backend": SLURM_MAIL_BACKEND,
        "recipients": tuple(str(item) for item in email.recipients),
        "dependency_job_ids": dependency_job_ids,
        "dependency_type": dependency_type,
        "mail_type": email.mail_type,
    }
    result = submit_slurm_mail_jobs(
        root=target,
        event=event,
        notification_plan=notification_plan,
        dependency_job_ids=dependency_job_ids,
        sbatch_path=sbatch_path,
        client=client,
        barrier_path_factory=barrier_path_factory,
        max_dependency_length=max_dependency_length,
    )
    if result.state != "submitted":
        record = NotificationSubmissionRecord(
            **base,
            state=result.state,
            scheduler_job_ids=result.scheduler_job_ids,
            sbatch_paths=result.sbatch_paths,
            barrier_job_ids=result.barrier_job_ids,
            reason=result.reason,
        )
        write_notification_record(target, record)
        append_notification_event(
            target,
            "notification_submit_failed",
            notification_event=event,
            backend=SLURM_MAIL_BACKEND,
            reason=result.reason,
            diagnostic_path=str(
                diagnostic_path(
                    target,
                    "notifications",
                    f"{event}_slurm_mail_submit_traceback.log",
                )
            ),
        )
        return record

    record = NotificationSubmissionRecord(
        **base,
        state="submitted",
        scheduler_job_ids=result.scheduler_job_ids,
        sbatch_paths=result.sbatch_paths,
        barrier_job_ids=result.barrier_job_ids,
        submitted_at=result.submitted_at,
    )
    write_notification_record(target, record)
    append_notification_event(
        target,
        "notification_submitted",
        notification_event=event,
        backend=SLURM_MAIL_BACKEND,
        scheduler_job_ids=result.scheduler_job_ids,
        barrier_job_ids=result.barrier_job_ids,
        dependency_job_ids=dependency_job_ids,
        recipients=tuple(str(item) for item in email.recipients),
    )
    return record
