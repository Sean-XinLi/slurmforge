from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ...errors import ConfigContractError
from .canonicalize import canonicalize_optional_path, resolve_optional_path
from .models import ModelCatalogEntry


def ensure_mapping(value: Any, name: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise ConfigContractError(f"{name} must be a mapping")
    return dict(value)


def entry_from_payload(project_root: Path, entry: dict[str, Any]) -> ModelCatalogEntry:
    name = str(entry.get("name", "")).strip()
    script_raw = entry.get("script")
    if not name or not script_raw:
        raise ConfigContractError("model_registry entry requires `name` and `script`")

    script = canonicalize_optional_path(project_root, script_raw)
    if script is None:
        raise ConfigContractError("model_registry entry requires non-empty `script`")

    return ModelCatalogEntry(
        name=name,
        script=script,
        yaml=canonicalize_optional_path(project_root, entry.get("yaml")),
        ddp_supported=bool(entry.get("ddp_supported", True)),
        ddp_required=bool(entry.get("ddp_required", False)),
        estimator_profile=str(entry.get("estimator_profile", "default")),
    )


def load_registry_entries(project_root: Path, registry_file_raw: Any) -> list[dict[str, Any]]:
    registry_path = resolve_optional_path(project_root, registry_file_raw)
    if registry_path is None:
        return []
    if not registry_path.exists():
        raise FileNotFoundError(f"model_registry.registry_file not found: {registry_path}")

    try:
        payload = yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ConfigContractError(f"Failed to parse model_registry.registry_file `{registry_path}`: {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigContractError("model_registry.registry_file must decode to a mapping")

    entries = payload.get("models", payload.get("extra_models", [])) or []
    if not isinstance(entries, list):
        raise ConfigContractError("model_registry.registry_file must provide a list under `models`")
    for entry in entries:
        if not isinstance(entry, dict):
            raise ConfigContractError("each model_registry.registry_file entry must be a mapping")
    return entries
