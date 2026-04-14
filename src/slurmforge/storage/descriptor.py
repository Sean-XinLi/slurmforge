"""Storage descriptor: meta/storage.json read/write.

Every batch writes this descriptor at materialization time. All read paths
consult it first; file-based engine detection is a legacy fallback only.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..identity import PACKAGE_NAME, __version__
from ..pipeline.config.api import StorageConfigSpec
from ..pipeline.config.codecs import normalize_storage_config, serialize_storage_config
from ..pipeline.records.io_utils import atomic_write_text

_DESCRIPTOR_FILENAME = "storage.json"
_DESCRIPTOR_SCHEMA_VERSION = 1


def build_storage_descriptor(
    storage_config: StorageConfigSpec,
    batch_root: Path,
) -> dict[str, Any]:
    """Build the ``meta/storage.json`` content."""
    from .backends.sqlite.connection import db_path_for_batch

    resolved_root = batch_root.resolve()
    engine = storage_config.backend.engine

    effective: dict[str, Any] = {"engine": engine}
    if engine == "sqlite":
        db_path = db_path_for_batch(batch_root, storage_config)
        try:
            effective["db_path_rel"] = db_path.relative_to(resolved_root).as_posix()
        except ValueError:
            effective["db_path_rel"] = str(db_path)

    return {
        "schema_version": _DESCRIPTOR_SCHEMA_VERSION,
        "storage": serialize_storage_config(storage_config),
        "effective": effective,
        "writer": {
            "name": PACKAGE_NAME,
            "version": __version__,
        },
    }


def write_storage_descriptor(
    staging_root: Path,
    storage_config: StorageConfigSpec,
    batch_root: Path,
) -> None:
    """Write ``storage.json`` into ``staging_root/meta/``."""
    meta_dir = staging_root / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    descriptor = build_storage_descriptor(storage_config, batch_root)
    atomic_write_text(
        meta_dir / _DESCRIPTOR_FILENAME,
        json.dumps(descriptor, indent=2, sort_keys=True),
    )


def read_storage_descriptor(batch_root: Path) -> StorageConfigSpec | None:
    """Read and parse ``meta/storage.json``.

    Returns None ONLY if the file does not exist (legacy batch).
    If the file exists but is corrupt, raises ``ConfigContractError`` — never
    silently falls back to guessing.
    """
    descriptor_path = batch_root.resolve() / "meta" / _DESCRIPTOR_FILENAME
    if not descriptor_path.exists():
        return None
    try:
        payload = json.loads(descriptor_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ConfigContractError(f"Corrupt storage descriptor at {descriptor_path}: {exc}")
    if not isinstance(payload, dict):
        raise ConfigContractError(f"Storage descriptor must be a mapping: {descriptor_path}")
    storage_section = payload.get("storage")
    if not isinstance(storage_section, dict):
        raise ConfigContractError(f"Storage descriptor missing 'storage' key: {descriptor_path}")
    return normalize_storage_config(storage_section)
