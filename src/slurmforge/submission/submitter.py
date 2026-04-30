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


def submit_prepared_stage_batch(
    prepared: PreparedSubmission,
    *,
    client: SlurmClientProtocol | None = None,
    mark_queued: bool = True,
    policy: Literal["new_only", "recover_partial"] = "new_only",
) -> dict[str, str]:
    if policy not in {"new_only", "recover_partial"}:
        raise ConfigContractError(f"Unsupported submission policy: {policy}")
    batch, ledger = load_ready_prepared_submission(prepared)
    batch_root = Path(prepared.batch_root)
    slurm = client or SlurmClient()
    group_job_ids = submitted_group_job_ids(batch_root)

    for group in batch.group_plans:
        record = ledger.groups[group.group_id]
        if record.scheduler_job_id and record.state in {"submitted", "adopted"}:
            if policy == "new_only":
                raise ConfigContractError(
                    f"Stage batch `{batch.stage_name}` already has submitted group `{group.group_id}` "
                    f"with scheduler job `{record.scheduler_job_id}`; submit a derived batch for a new execution"
                )
            group_job_ids[group.group_id] = record.scheduler_job_id
            continue
        if record.state == "submitting" and not record.scheduler_job_id:
            ledger.state = "uncertain"
            record.reason = (
                "group may have reached sbatch without a recorded scheduler job id"
            )
            write_submission_ledger(batch_root, ledger)
            raise ConfigContractError(
                f"Submission ledger for `{batch.stage_name}` is uncertain at group `{group.group_id}`; "
                "manual reconcile is required before retrying"
            )
        dependency = dependency_for(group.group_id, batch, group_job_ids)
        record.state = "submitting"
        record.dependency = dependency
        record.reason = ""
        ledger.state = "partial" if group_job_ids else "submitting"
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
            record.state = "failed"
            record.reason = str(exc)
            ledger.state = "failed"
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
        record.scheduler_job_id = job_id
        record.submitted_at = utc_now()
        record.state = "submitted"
        record.reason = ""
        group_job_ids[group.group_id] = job_id
        ledger.state = "partial"
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

    ledger.state = "submitted"
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
