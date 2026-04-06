from __future__ import annotations

import shlex
from pathlib import Path
from typing import Iterable

from ..records.io_utils import atomic_write_text
from .cache import write_train_outputs_manifest
from .contract import build_train_outputs_manifest
from .models import TrainOutputsManifest


def render_train_outputs_env(*, manifest_path: Path, manifest: TrainOutputsManifest) -> str:
    exports = {
        "AI_INFRA_TRAIN_ARTIFACT_MANIFEST": str(manifest_path.expanduser().resolve()),
        "AI_INFRA_PRIMARY_CHECKPOINT": str(manifest.primary_checkpoint or ""),
        "AI_INFRA_BEST_CHECKPOINT": str(manifest.best_checkpoint or ""),
        "AI_INFRA_LATEST_CHECKPOINT": str(manifest.latest_checkpoint or ""),
    }
    return "\n".join(f"export {key}={shlex.quote(value)}" for key, value in exports.items()) + "\n"


def write_train_outputs_contract(
    *,
    result_dir: Path,
    manifest_path: Path,
    env_path: Path,
    checkpoint_globs: list[str] | tuple[str, ...],
    run_id: str,
    model_name: str,
    primary_policy: str = "latest",
    explicit_checkpoint: str | None = None,
    workdirs: Iterable[Path | str] | None = None,
    max_matches_per_glob: int = 500,
) -> TrainOutputsManifest:
    manifest = build_train_outputs_manifest(
        result_dir=result_dir,
        checkpoint_globs=checkpoint_globs,
        run_id=run_id,
        model_name=model_name,
        primary_policy=primary_policy,
        explicit_checkpoint=explicit_checkpoint,
        workdirs=workdirs,
        max_matches_per_glob=max_matches_per_glob,
    )
    write_train_outputs_manifest(manifest_path, manifest)
    atomic_write_text(env_path, render_train_outputs_env(manifest_path=manifest_path, manifest=manifest))
    return manifest
