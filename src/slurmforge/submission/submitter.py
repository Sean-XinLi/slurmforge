from __future__ import annotations

from pathlib import Path
from typing import Literal

from ..errors import ConfigContractError
from ..io import diagnostic_path, utc_now, write_exception_diagnostic
from ..slurm import SlurmClient, SlurmClientProtocol, SlurmSubmitOptions
from .dependencies import dependency_for
from .ledger import (
    append_submission_event,
    submitted_group_job_ids,
    write_submission_ledger,
)
from .models import PreparedSubmission
from .queueing import mark_stage_batch_queued
from .ready import load_ready_prepared_submission
from .submit_policy import (
    adopt_existing_group_job,
    require_retryable_group,
    validate_submit_policy,
)
from .submit_transition import (
    mark_batch_submitted,
    mark_group_failed,
    mark_group_submitted,
    mark_group_submitting,
    mark_group_uncertain,
)


def submit_prepared_stage_batch(
    prepared: PreparedSubmission,
    *,
    client: SlurmClientProtocol | None = None,
    mark_queued: bool = True,
    policy: Literal["new_only", "recover_partial"] = "new_only",
) -> dict[str, str]:
    validate_submit_policy(policy)
    batch, ledger = load_ready_prepared_submission(prepared)
    batch_root = Path(prepared.batch_root)
    slurm = client or SlurmClient()
    group_job_ids = submitted_group_job_ids(batch_root)

    for group in batch.group_plans:
        record = ledger.groups[group.group_id]
        if adopt_existing_group_job(
            stage_name=batch.stage_name,
            group_id=group.group_id,
            record=record,
            policy=policy,
            group_job_ids=group_job_ids,
        ):
            continue
        try:
            require_retryable_group(record, stage_name=batch.stage_name)
        except ConfigContractError:
            mark_group_uncertain(ledger, record)
            write_submission_ledger(batch_root, ledger)
            raise
        dependency = dependency_for(group.group_id, batch, group_job_ids)
        mark_group_submitting(
            ledger,
            record,
            dependency=dependency,
            submitted_group_count=len(group_job_ids),
        )
        write_submission_ledger(batch_root, ledger)
        append_submission_event(
            batch_root,
            "group_submit_started",
            stage=batch.stage_name,
            group_id=group.group_id,
            sbatch_path=record.sbatch_path,
            dependency=dependency,
        )
        try:
            job_id = slurm.submit(
                Path(record.sbatch_path),
                options=SlurmSubmitOptions(dependency=dependency or ""),
            )
        except Exception as exc:
            diagnostic = write_exception_diagnostic(
                diagnostic_path(
                    batch_root,
                    "submissions",
                    "diagnostics",
                    f"{group.group_id}_submit_traceback.log",
                ),
                exc,
            )
            mark_group_failed(ledger, record, reason=str(exc))
            write_submission_ledger(batch_root, ledger)
            append_submission_event(
                batch_root,
                "group_submit_failed",
                stage=batch.stage_name,
                group_id=group.group_id,
                reason=str(exc),
                diagnostic_path=str(diagnostic),
            )
            raise
        mark_group_submitted(ledger, record, job_id=job_id, submitted_at=utc_now())
        group_job_ids[group.group_id] = job_id
        write_submission_ledger(batch_root, ledger)
        append_submission_event(
            batch_root,
            "group_submitted",
            stage=batch.stage_name,
            group_id=group.group_id,
            scheduler_job_id=job_id,
            sbatch_path=record.sbatch_path,
            dependency=dependency,
        )

    mark_batch_submitted(ledger)
    write_submission_ledger(batch_root, ledger)
    append_submission_event(
        batch_root,
        "batch_submitted",
        stage=batch.stage_name,
        scheduler_job_ids=list(group_job_ids.values()),
    )
    if mark_queued:
        mark_stage_batch_queued(batch, group_job_ids)
    return group_job_ids
