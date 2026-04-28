from __future__ import annotations

from pathlib import Path

from ..root_model import is_stage_batch_root, is_train_eval_pipeline_root
from ..slurm import SlurmClient
from ..status import reconcile_stage_batch_with_slurm
from ..storage.loader import load_execution_stage_batch_plan
from .ledger import submitted_group_job_ids


def reconcile_batch_submission(
    batch_root: Path,
    *,
    client: SlurmClient | None = None,
    missing_output_grace_seconds: int = 300,
) -> dict[str, str]:
    group_job_ids = submitted_group_job_ids(batch_root)
    if not group_job_ids:
        return {}
    batch = load_execution_stage_batch_plan(batch_root)
    reconcile_stage_batch_with_slurm(
        batch,
        group_job_ids=group_job_ids,
        client=client or SlurmClient(),
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    return group_job_ids


def reconcile_root_submissions(
    root: Path,
    *,
    stage: str | None = None,
    client: SlurmClient | None = None,
    missing_output_grace_seconds: int = 300,
) -> None:
    if is_stage_batch_root(root):
        batch = load_execution_stage_batch_plan(root)
        if stage is None or batch.stage_name == stage:
            reconcile_batch_submission(
                root,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
        return
    if is_train_eval_pipeline_root(root):
        for stage_root in sorted((root / "stage_batches").glob("*")):
            if not is_stage_batch_root(stage_root):
                continue
            batch = load_execution_stage_batch_plan(stage_root)
            if stage is not None and batch.stage_name != stage:
                continue
            reconcile_batch_submission(
                stage_root,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
