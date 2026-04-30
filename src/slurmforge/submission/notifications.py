from __future__ import annotations

from pathlib import Path

from ..config_contract.option_sets import EMAIL_EVENT_BATCH_FINISHED
from ..emit.stage import (
    write_stage_notification_barrier_file,
    write_stage_notification_submit_file,
)
from ..errors import ConfigContractError
from ..notifications.models import NotificationSubmissionRecord
from ..notifications.policy import email_notification_enabled
from ..plans.stage import StageBatchPlan
from ..slurm import SlurmClient, SlurmClientProtocol
from .dependency_tree import MAX_DEPENDENCY_LENGTH, dependency_sink_group_ids
from .notification_mail import submit_slurm_mail_notification


def submit_stage_batch_notification(
    batch: StageBatchPlan,
    group_job_ids: dict[str, str],
    *,
    client: SlurmClientProtocol | None = None,
    event: str = EMAIL_EVENT_BATCH_FINISHED,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> NotificationSubmissionRecord | None:
    if not email_notification_enabled(batch.notification_plan, event):
        return None
    dependency_group_ids = dependency_sink_group_ids(batch)
    missing = [
        group_id for group_id in dependency_group_ids if group_id not in group_job_ids
    ]
    if missing:
        raise ConfigContractError(
            f"Cannot submit notification before groups have scheduler ids: {', '.join(missing)}"
        )
    dependency_job_ids = tuple(
        group_job_ids[group_id] for group_id in dependency_group_ids
    )
    notification_path = write_stage_notification_submit_file(batch, event)
    return submit_slurm_mail_notification(
        root=Path(batch.submission_root),
        root_kind="stage_batch",
        event=event,
        notification_plan=batch.notification_plan,
        dependency_job_ids=dependency_job_ids,
        sbatch_path=notification_path,
        client=client or SlurmClient(),
        barrier_path_factory=lambda barrier_index: write_stage_notification_barrier_file(
            batch,
            event,
            barrier_index=barrier_index,
        ),
        max_dependency_length=max_dependency_length,
    )
