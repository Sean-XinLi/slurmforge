from __future__ import annotations

from pathlib import Path

from ..storage.materialization import read_materialization_status
from .ledger import read_submission_ledger, submitted_group_job_ids
from .models import SubmissionGroupState, SubmissionState


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
