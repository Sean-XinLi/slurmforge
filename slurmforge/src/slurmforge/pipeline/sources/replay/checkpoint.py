from __future__ import annotations

from pathlib import Path

from ...status import read_latest_result_dir
from ...train_outputs import load_or_build_train_outputs_manifest
from .models import RetryCandidate


def resolve_retry_checkpoint(candidate: RetryCandidate) -> Path | None:
    run_dir = Path(candidate.plan.run_dir)
    latest_result_dir = read_latest_result_dir(run_dir)
    if latest_result_dir is None or not latest_result_dir.exists():
        return None

    manifest = load_or_build_train_outputs_manifest(
        result_dir=latest_result_dir,
        checkpoint_globs=tuple(candidate.plan.artifacts.checkpoint_globs),
        run_id=candidate.plan.run_id,
        model_name=candidate.plan.model_name,
        primary_policy=candidate.plan.eval_train_outputs.checkpoint_policy,
        explicit_checkpoint=candidate.plan.eval_train_outputs.explicit_checkpoint,
        persist=True,
    )
    if not manifest.primary_checkpoint:
        return None
    return Path(manifest.primary_checkpoint).resolve()
