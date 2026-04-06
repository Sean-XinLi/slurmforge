from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ...errors import ConfigContractError
from ..checkpoints import discover_checkpoint_files, extract_checkpoint_step
from .discovery import (
    collect_checkpoint_artifacts,
    resolve_explicit_checkpoint_path,
    resolve_latest_checkpoint,
)
from .models import TrainOutputsManifest
from .selection import (
    best_checkpoint_path,
    normalize_explicit_checkpoint,
    normalize_primary_policy,
    resolve_primary_checkpoint,
)


def build_train_outputs_manifest(
    *,
    result_dir: Path,
    checkpoint_globs: list[str] | tuple[str, ...],
    run_id: str,
    model_name: str,
    primary_policy: str = "latest",
    explicit_checkpoint: str | None = None,
    workdirs: Iterable[Path | str] | None = None,
    max_matches_per_glob: int = 500,
) -> TrainOutputsManifest:
    normalized_policy = normalize_primary_policy(primary_policy)
    normalized_explicit_checkpoint = normalize_explicit_checkpoint(explicit_checkpoint)
    if normalized_policy == "explicit" and normalized_explicit_checkpoint is None:
        raise ConfigContractError("explicit primary_policy requires explicit_checkpoint")
    if normalized_policy != "explicit" and normalized_explicit_checkpoint is not None:
        raise ConfigContractError("explicit_checkpoint is only valid when primary_policy=explicit")

    resolved_result_dir = result_dir.expanduser().resolve()
    checkpoint_dir = (resolved_result_dir / "checkpoints").resolve()
    collect_checkpoint_artifacts(
        result_dir=resolved_result_dir,
        workdirs=workdirs,
        checkpoint_globs=checkpoint_globs,
        max_matches_per_glob=max_matches_per_glob,
    )
    latest_checkpoint, latest_selection_reason, selection_error = resolve_latest_checkpoint(
        result_dir=resolved_result_dir,
        checkpoint_globs=checkpoint_globs,
    )
    candidates = discover_checkpoint_files(resolved_result_dir, checkpoint_globs)
    best_checkpoint = best_checkpoint_path(
        candidates,
        root=resolved_result_dir,
        extract_checkpoint_step=extract_checkpoint_step,
    )

    explicit_resolved_checkpoint: Path | None = None
    if normalized_explicit_checkpoint is not None:
        try:
            explicit_resolved_checkpoint, explicit_reason = resolve_explicit_checkpoint_path(
                explicit_checkpoint=normalized_explicit_checkpoint,
                result_dir=resolved_result_dir,
                checkpoint_dir=checkpoint_dir,
            )
        except FileNotFoundError as exc:
            selection_error = str(exc) if not selection_error else f"{selection_error}; {exc}"
            explicit_reason = ""
    else:
        explicit_reason = ""

    primary_checkpoint, selection_reason = resolve_primary_checkpoint(
        primary_policy=normalized_policy,
        latest_checkpoint=latest_checkpoint,
        best_checkpoint=best_checkpoint,
        explicit_checkpoint=explicit_resolved_checkpoint,
        latest_selection_reason=latest_selection_reason,
    )
    if normalized_policy == "explicit" and selection_reason == "" and explicit_reason:
        selection_reason = explicit_reason
    status = "ok" if primary_checkpoint is not None else ("selection_error" if selection_error else "no_checkpoint")

    return TrainOutputsManifest(
        run_id=str(run_id),
        model_name=str(model_name),
        result_dir=str(resolved_result_dir),
        checkpoint_dir=str(checkpoint_dir),
        primary_policy=normalized_policy,
        explicit_checkpoint=normalized_explicit_checkpoint or "",
        primary_checkpoint="" if primary_checkpoint is None else str(primary_checkpoint),
        latest_checkpoint="" if latest_checkpoint is None else str(latest_checkpoint),
        best_checkpoint="" if best_checkpoint is None else str(best_checkpoint),
        selection_reason=selection_reason,
        selection_error=selection_error,
        status=status,
    )
