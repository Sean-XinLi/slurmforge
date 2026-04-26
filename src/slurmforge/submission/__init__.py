from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClient
from ..storage import read_materialization_status
from .generation import create_submit_generation, prepare_stage_submission
from .ledger import read_submission_ledger, submitted_group_job_ids, write_submission_ledger
from .models import (
    GroupSubmissionRecord,
    PreparedSubmission,
    SubmissionGroupState,
    SubmissionLedger,
    SubmissionState,
    SubmitGeneration,
)
from .reconcile import reconcile_batch_submission_impl, reconcile_root_submissions_impl
from .submitter import submit_prepared_stage_batch


def read_submission_state(batch_root: Path) -> SubmissionState:
    root = Path(batch_root)
    ledger = read_submission_ledger(root)
    materialization = read_materialization_status(root)
    groups: tuple[SubmissionGroupState, ...] = ()
    if ledger is not None:
        groups = tuple(
            SubmissionGroupState(
                group_id=record.group_id,
                sbatch_path=record.sbatch_path,
                dependency=record.dependency,
                scheduler_job_id=record.scheduler_job_id,
                state=record.state,
                submitted_at=record.submitted_at,
                reason=record.reason,
            )
            for record in sorted(ledger.groups.values(), key=lambda item: item.group_id)
        )
    return SubmissionState(
        batch_root=root,
        batch_id="" if ledger is None else ledger.batch_id,
        stage_name="" if ledger is None else ledger.stage_name,
        generation_id="" if ledger is None else ledger.generation_id,
        ledger_state="missing" if ledger is None else ledger.state,
        materialization_state="missing" if materialization is None else materialization.state,
        submitted_group_job_ids=submitted_group_job_ids(root),
        groups=groups,
    )


def reconcile_batch_submission(
    batch_root: Path,
    *,
    client: SlurmClient | None = None,
    missing_output_grace_seconds: int = 300,
) -> dict[str, str]:
    return reconcile_batch_submission_impl(
        Path(batch_root),
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )


def reconcile_root_submissions(
    root: Path,
    *,
    stage: str | None = None,
    client: SlurmClient | None = None,
    missing_output_grace_seconds: int = 300,
) -> None:
    reconcile_root_submissions_impl(
        Path(root),
        stage=stage,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )


__all__ = [
    "GroupSubmissionRecord",
    "PreparedSubmission",
    "SubmissionGroupState",
    "SubmissionLedger",
    "SubmissionState",
    "SubmitGeneration",
    "create_submit_generation",
    "prepare_stage_submission",
    "read_submission_ledger",
    "read_submission_state",
    "reconcile_batch_submission",
    "reconcile_root_submissions",
    "submit_prepared_stage_batch",
    "write_submission_ledger",
]
