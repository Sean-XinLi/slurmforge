from __future__ import annotations

from pathlib import Path

from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..slurm import SlurmClientProtocol
from ..submission.reconcile import reconcile_batch_submission


def reconcile_eval_shard(
    pipeline_root: Path,
    shard_root: Path,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
) -> None:
    reconcile_batch_submission(
        shard_root,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    refresh_stage_batch_status(shard_root)
    refresh_train_eval_pipeline_status(pipeline_root)
