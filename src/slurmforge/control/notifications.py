from __future__ import annotations

from pathlib import Path

from ..config_contract.option_sets import EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
from ..emit.pipeline_notification import (
    write_pipeline_notification_barrier_file,
    write_pipeline_notification_submit_file,
)
from ..notifications.models import NotificationSubmissionRecord
from ..notifications.policy import email_notification_enabled
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..submission.notification_mail import submit_slurm_mail_notification


def submit_pipeline_terminal_notification(
    pipeline_root: Path,
    plan,
    *,
    final_gate_job_id: str,
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> NotificationSubmissionRecord | None:
    event = EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED
    if not email_notification_enabled(plan.notification_plan, event):
        return None
    notification_path = write_pipeline_notification_submit_file(plan, event=event)
    return submit_slurm_mail_notification(
        root=pipeline_root,
        root_kind="train_eval_pipeline",
        event=event,
        notification_plan=plan.notification_plan,
        dependency_job_ids=(final_gate_job_id,),
        sbatch_path=notification_path,
        client=client,
        barrier_path_factory=lambda barrier_index: write_pipeline_notification_barrier_file(
            plan,
            event=event,
            barrier_index=barrier_index,
        ),
        max_dependency_length=max_dependency_length,
    )
