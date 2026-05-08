from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..io import SchemaVersion, read_json
from ..workflow_contract import TRAIN_EVAL_PIPELINE_KIND
from .models import RootKind, STAGE_BATCH_KIND, root_kind_from_manifest


@dataclass(frozen=True)
class RootManifestRecord:
    kind: RootKind
    payload: dict[str, Any]
    schema_version: int


def root_manifest_path(root: Path) -> Path:
    return Path(root) / "manifest.json"


def read_root_manifest(root: Path) -> RootManifestRecord | None:
    path = root_manifest_path(root)
    if not path.exists():
        return None
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ConfigContractError(f"root manifest must contain a mapping: {path}")
    if "kind" not in payload:
        raise ConfigContractError(f"root manifest kind is required: {path}")
    kind_value = payload["kind"]
    if not isinstance(kind_value, str):
        raise ConfigContractError(f"root manifest kind must be a string: {path}")
    kind = root_kind_from_manifest(kind_value)
    if kind is None:
        raise ConfigContractError(
            f"root manifest kind is unsupported: {kind_value}"
        )
    schema_version = _manifest_schema_version(payload, path=path, kind=kind)
    return RootManifestRecord(
        kind=kind,
        payload=dict(payload),
        schema_version=schema_version,
    )


def require_root_manifest(root: Path) -> RootManifestRecord:
    record = read_root_manifest(root)
    if record is None:
        raise ConfigContractError(
            f"not a stage batch or train/eval pipeline root: {Path(root).resolve()}"
        )
    return record


def _manifest_schema_version(
    payload: dict[str, Any], *, path: Path, kind: RootKind
) -> int:
    if "schema_version" not in payload:
        raise ConfigContractError(f"root manifest schema_version is required: {path}")
    schema_version = payload["schema_version"]
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ConfigContractError(
            f"root manifest schema_version must be an integer: {path}"
        )
    expected = _expected_schema_version(kind)
    if schema_version != expected:
        raise ConfigContractError(
            f"root manifest schema_version is not supported: {schema_version}"
        )
    return schema_version


def _expected_schema_version(kind: RootKind) -> int:
    if kind == STAGE_BATCH_KIND:
        return SchemaVersion.BATCH_MANIFEST
    if kind == TRAIN_EVAL_PIPELINE_KIND:
        return SchemaVersion.PIPELINE_MANIFEST
    raise ConfigContractError(f"unsupported root manifest kind: {kind}")
