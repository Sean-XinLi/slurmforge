from __future__ import annotations

from pathlib import Path

from ..emit.stage import (
    write_stage_notification_barrier_file,
    write_stage_notification_submit_file,
)
from ..errors import ConfigContractError
from ..io import utc_now
from ..notifications.models import NotificationDeliveryRecord
from ..notifications.policy import email_notification_enabled
from ..notifications.records import append_notification_event, write_notification_record
from ..plans.stage import StageBatchPlan
from ..slurm import SlurmClient, SlurmClientProtocol


MAX_DEPENDENCY_LENGTH = 3500


def finalizer_dependency_group_ids(batch: StageBatchPlan) -> tuple[str, ...]:
    group_ids = {group.group_id for group in batch.group_plans}
    has_outgoing_dependency: set[str] = set()
    for dep in batch.budget_plan.dependencies:
        has_outgoing_dependency.update(str(item) for item in dep.from_groups if item)
    sinks = sorted(group_ids - has_outgoing_dependency)
    return tuple(sinks or sorted(group_ids))


def _dependency(job_ids: tuple[str, ...]) -> str:
    return f"afterany:{':'.join(job_ids)}"


def _dependency_chunks(
    job_ids: tuple[str, ...], *, max_length: int
) -> tuple[tuple[str, ...], ...]:
    chunks: list[tuple[str, ...]] = []
    current: list[str] = []
    for job_id in job_ids:
        candidate = tuple([*current, job_id])
        if current and len(_dependency(candidate)) > max_length:
            chunks.append(tuple(current))
            current = [job_id]
            continue
        if len(_dependency((job_id,))) > max_length:
            raise ConfigContractError(
                f"Scheduler job id is too long for a dependency expression: {job_id}"
            )
        current.append(job_id)
    if current:
        chunks.append(tuple(current))
    return tuple(chunks)


def _submit_with_dependency_tree(
    *,
    batch: StageBatchPlan,
    event: str,
    finalizer_path: Path,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> tuple[str, tuple[str, ...]]:
    current = dependency_job_ids
    barrier_job_ids: list[str] = []
    barrier_index = 1
    while len(_dependency(current)) > max_dependency_length:
        next_level: list[str] = []
        for chunk in _dependency_chunks(current, max_length=max_dependency_length):
            barrier_path = write_stage_notification_barrier_file(
                batch,
                event,
                barrier_index=barrier_index,
            )
            barrier_index += 1
            barrier_id = client.submit(barrier_path, dependency=_dependency(chunk))
            barrier_job_ids.append(barrier_id)
            next_level.append(barrier_id)
        current = tuple(next_level)
    finalizer_job_id = client.submit(finalizer_path, dependency=_dependency(current))
    return finalizer_job_id, tuple(barrier_job_ids)


def submit_stage_batch_finalizer(
    batch: StageBatchPlan,
    group_job_ids: dict[str, str],
    *,
    client: SlurmClientProtocol | None = None,
    event: str = "batch_finished",
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> NotificationDeliveryRecord | None:
    if not email_notification_enabled(batch.notification_plan, event):
        return None
    dependency_group_ids = finalizer_dependency_group_ids(batch)
    missing = [
        group_id for group_id in dependency_group_ids if group_id not in group_job_ids
    ]
    if missing:
        raise ConfigContractError(
            f"Cannot submit notification finalizer before groups have scheduler ids: {', '.join(missing)}"
        )
    dependency_job_ids = tuple(
        group_job_ids[group_id] for group_id in dependency_group_ids
    )
    finalizer_path = write_stage_notification_submit_file(batch, event)
    slurm = client or SlurmClient()
    finalizer_job_id, barrier_job_ids = _submit_with_dependency_tree(
        batch=batch,
        event=event,
        finalizer_path=finalizer_path,
        dependency_job_ids=dependency_job_ids,
        client=slurm,
        max_dependency_length=max_dependency_length,
    )
    record = NotificationDeliveryRecord(
        event=event,
        root_kind="stage_batch",
        root=batch.submission_root,
        backend="email",
        state="submitted",
        recipients=tuple(str(item) for item in batch.notification_plan.email.to),
        scheduler_job_id=finalizer_job_id,
        sbatch_path=str(finalizer_path),
        barrier_job_ids=barrier_job_ids,
        dependency_job_ids=dependency_job_ids,
        submitted_at=utc_now(),
    )
    batch_root = Path(batch.submission_root)
    write_notification_record(batch_root, record)
    append_notification_event(
        batch_root,
        "notification_finalizer_submitted",
        notification_event=event,
        scheduler_job_id=finalizer_job_id,
        barrier_job_ids=barrier_job_ids,
        dependency_job_ids=dependency_job_ids,
    )
    return record
