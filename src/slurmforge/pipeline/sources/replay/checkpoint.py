from __future__ import annotations

from pathlib import Path

from ...train_outputs import load_or_build_train_outputs_manifest
from .models import RetryCandidate


def resolve_retry_checkpoint(candidate: RetryCandidate) -> Path | None:
    latest_result_dir_raw = str(candidate.latest_result_dir or "").strip()
    if not latest_result_dir_raw:
        return None

    latest_result_dir = Path(latest_result_dir_raw).resolve()
    if not latest_result_dir.exists():
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
