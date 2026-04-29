from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..io import read_json
from .models import RootDescriptor, root_kind_from_manifest


def _manifest_path(root: Path) -> Path:
    return Path(root) / "manifest.json"


def _read_manifest(root: Path) -> dict[str, Any] | None:
    path = _manifest_path(root)
    if not path.exists():
        return None
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ConfigContractError(f"root manifest must contain a mapping: {path}")
    return payload


def detect_root(root: Path) -> RootDescriptor:
    target = Path(root).resolve()
    if not target.exists():
        raise ConfigContractError(f"root does not exist: {target}")
    manifest = _read_manifest(target)
    if manifest is None:
        raise ConfigContractError(
            f"not a stage batch or train/eval pipeline root: {target}"
        )
    kind = root_kind_from_manifest(manifest.get("kind"))
    if kind is None:
        raise ConfigContractError(
            f"not a stage batch or train/eval pipeline root: {target}"
        )
    return RootDescriptor(root=target, kind=kind, manifest=manifest)


def is_stage_batch_root(root: Path) -> bool:
    manifest = _read_manifest(Path(root))
    return (
        root_kind_from_manifest(None if manifest is None else manifest.get("kind"))
        == "stage_batch"
    )


def is_train_eval_pipeline_root(root: Path) -> bool:
    manifest = _read_manifest(Path(root))
    return (
        root_kind_from_manifest(None if manifest is None else manifest.get("kind"))
        == "train_eval_pipeline"
    )
