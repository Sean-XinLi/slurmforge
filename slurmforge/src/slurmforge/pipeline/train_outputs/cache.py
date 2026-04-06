from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ..records.io_utils import atomic_write_text
from .codec import deserialize_train_outputs_manifest, serialize_train_outputs_manifest
from .contract import build_train_outputs_manifest
from .models import TrainOutputsManifest
from .paths import train_outputs_manifest_path_for_result_dir
from .selection import normalize_explicit_checkpoint, normalize_primary_policy


def read_train_outputs_manifest(result_dir: Path) -> TrainOutputsManifest | None:
    path = train_outputs_manifest_path_for_result_dir(result_dir)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return deserialize_train_outputs_manifest(payload)


def write_train_outputs_manifest(path: Path, manifest: TrainOutputsManifest) -> None:
    atomic_write_text(path, json.dumps(serialize_train_outputs_manifest(manifest), indent=2, sort_keys=True))


def load_or_build_train_outputs_manifest(
    *,
    result_dir: Path,
    checkpoint_globs: list[str] | tuple[str, ...],
    run_id: str,
    model_name: str,
    primary_policy: str = "latest",
    explicit_checkpoint: str | None = None,
    workdirs: Iterable[Path | str] | None = None,
    max_matches_per_glob: int = 500,
    persist: bool = True,
) -> TrainOutputsManifest:
    normalized_policy = normalize_primary_policy(primary_policy)
    normalized_explicit_checkpoint = normalize_explicit_checkpoint(explicit_checkpoint) or ""
    manifest = read_train_outputs_manifest(result_dir)
    if (
        manifest is not None
        and manifest.primary_policy == normalized_policy
        and manifest.explicit_checkpoint == normalized_explicit_checkpoint
        and manifest.primary_checkpoint
        and Path(manifest.primary_checkpoint).exists()
    ):
        return manifest

    manifest = build_train_outputs_manifest(
        result_dir=result_dir,
        checkpoint_globs=checkpoint_globs,
        run_id=run_id,
        model_name=model_name,
        primary_policy=normalized_policy,
        explicit_checkpoint=normalized_explicit_checkpoint or None,
        workdirs=workdirs,
        max_matches_per_glob=max_matches_per_glob,
    )
    if persist:
        write_train_outputs_manifest(train_outputs_manifest_path_for_result_dir(result_dir), manifest)
    return manifest
