from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from .manifest import read_root_manifest, require_root_manifest
from .models import RootDescriptor


def detect_root(root: Path) -> RootDescriptor:
    target = Path(root).resolve()
    if not target.exists():
        raise ConfigContractError(f"root does not exist: {target}")
    manifest = require_root_manifest(target)
    return RootDescriptor(
        root=target,
        kind=manifest.kind,
        schema_version=manifest.schema_version,
    )


def is_stage_batch_root(root: Path) -> bool:
    manifest = read_root_manifest(Path(root))
    return manifest is not None and manifest.kind == "stage_batch"


def is_train_eval_pipeline_root(root: Path) -> bool:
    manifest = read_root_manifest(Path(root))
    return manifest is not None and manifest.kind == "train_eval_pipeline"
