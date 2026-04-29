from __future__ import annotations

from pathlib import Path

from ..io import diagnostic_path, utc_now, write_exception_diagnostic
from .email import send_email_summary
from .models import NotificationDeliveryRecord, NotificationSummaryInput
from .policy import email_notification_enabled
from .records import (
    append_notification_event,
    read_notification_record,
    write_notification_record,
)
from .summary import build_notification_summary, render_summary_text


def notification_subject(notification_plan, summary) -> str:
    prefix = notification_plan.email.subject_prefix or "SlurmForge"
    noun = (
        "train/eval pipeline"
        if summary.root_kind == "train_eval_pipeline"
        else "stage batch"
    )
    return f"{prefix} {noun} finished: {summary.project}/{summary.experiment} {summary.state}"


def deliver_notification(
    root: Path,
    *,
    event: str,
    notification_plan,
    summary_input: NotificationSummaryInput,
) -> NotificationDeliveryRecord | None:
    target = Path(root)
    if not email_notification_enabled(notification_plan, event):
        return None
    existing = read_notification_record(target, event, "email")
    if existing is not None and existing.state == "sent":
        return existing
    summary = build_notification_summary(summary_input)
    email = notification_plan.email
    recipients = tuple(str(item) for item in email.to)
    subject = notification_subject(notification_plan, summary)
    body = render_summary_text(summary)
    base = {
        "event": event,
        "root_kind": summary.root_kind,
        "root": str(target),
        "backend": "email",
        "recipients": recipients,
        "subject": subject,
        "scheduler_job_id": "" if existing is None else existing.scheduler_job_id,
        "sbatch_path": "" if existing is None else existing.sbatch_path,
        "barrier_job_ids": () if existing is None else existing.barrier_job_ids,
        "dependency_job_ids": () if existing is None else existing.dependency_job_ids,
        "submitted_at": "" if existing is None else existing.submitted_at,
    }
    try:
        send_email_summary(
            sender=email.from_address or "slurmforge@localhost",
            recipients=recipients,
            subject=subject,
            body=body,
            sendmail=email.sendmail or "/usr/sbin/sendmail",
        )
    except Exception as exc:
        diagnostic = write_exception_diagnostic(
            diagnostic_path(target, "notifications", f"{event}_email_traceback.log"),
            exc,
        )
        record = NotificationDeliveryRecord(**base, state="failed", reason=str(exc))
        write_notification_record(target, record)
        append_notification_event(
            target,
            "notification_failed",
            notification_event=event,
            reason=str(exc),
            diagnostic_path=str(diagnostic),
        )
        return record
    record = NotificationDeliveryRecord(**base, state="sent", sent_at=utc_now())
    write_notification_record(target, record)
    append_notification_event(
        target, "notification_sent", notification_event=event, recipients=recipients
    )
    return record
