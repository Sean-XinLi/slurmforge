from __future__ import annotations

from dataclasses import dataclass, field, replace
import json
from pathlib import Path
from typing import Any

from ...errors import ConfigContractError
from .canonicalize import resolve_optional_path
from .merge import catalog_from_entry_map, duplicate_model_names, entry_by_name, ensure_unique_entries, merge_catalog_layers
from .models import ModelCatalogEntry, ModelSpec, ResolvedModelCatalog
from .registry_loader import ensure_mapping, entry_from_payload, load_registry_entries


def validate_catalog(catalog: ResolvedModelCatalog) -> ResolvedModelCatalog:
    duplicates = duplicate_model_names(catalog.entries)
    if duplicates:
        joined = ", ".join(duplicates)
        raise ConfigContractError(f"resolved_model_catalog contains duplicate model names: {joined}")
    return catalog


def resolve_catalog_entry(entry: ModelCatalogEntry, *, project_root: Path) -> ModelSpec:
    script_path = resolve_optional_path(project_root, entry.script)
    if script_path is None:
        raise ConfigContractError(f"model_catalog entry `{entry.name}` requires non-empty `script`")
    return ModelSpec(
        name=entry.name,
        script=script_path,
        yaml_path=resolve_optional_path(project_root, entry.yaml),
        ddp_supported=bool(entry.ddp_supported),
        ddp_required=bool(entry.ddp_required),
        estimator_profile=str(entry.estimator_profile),
    )


def resolve_model_spec(
    catalog: ResolvedModelCatalog,
    model_cfg: dict[str, Any],
    *,
    project_root: Path,
) -> ModelSpec:
    if not isinstance(model_cfg, dict):
        raise ConfigContractError("model config must be a mapping")
    name = str(model_cfg.get("name", "")).strip()
    if not name:
        raise ConfigContractError("model.name is required")

    base_entry = entry_by_name(catalog.entries, name)
    if base_entry is None:
        script_raw = model_cfg.get("script")
        if not script_raw:
            raise ConfigContractError(
                f"Unknown model.name={name}. Provide `model.script` or register it via "
                "model_registry.registry_file / model_registry.extra_models."
            )
        script = resolve_optional_path(project_root, script_raw)
        if script is None:
            raise ConfigContractError("model.script must be non-empty")
        return ModelSpec(
            name=name,
            script=script,
            yaml_path=resolve_optional_path(project_root, model_cfg.get("yaml")),
            ddp_supported=bool(model_cfg.get("ddp_supported", True)),
            ddp_required=bool(model_cfg.get("ddp_required", False)),
            estimator_profile=str(model_cfg.get("estimator_profile", "default")),
        )

    base = resolve_catalog_entry(base_entry, project_root=project_root)
    script = base.script
    if model_cfg.get("script"):
        resolved_script = resolve_optional_path(project_root, model_cfg.get("script"))
        if resolved_script is not None:
            script = resolved_script

    yaml_path = base.yaml_path
    if model_cfg.get("yaml"):
        resolved_yaml = resolve_optional_path(project_root, model_cfg.get("yaml"))
        if resolved_yaml is not None:
            yaml_path = resolved_yaml

    return replace(
        base,
        script=script,
        yaml_path=yaml_path,
        ddp_supported=bool(model_cfg.get("ddp_supported", base.ddp_supported)),
        ddp_required=bool(model_cfg.get("ddp_required", base.ddp_required)),
        estimator_profile=str(model_cfg.get("estimator_profile", base.estimator_profile)),
    )


def build_model_catalog(
    project_root: Path,
    *,
    model_registry_cfg: dict[str, Any] | None = None,
    model_catalog_cfg: dict[str, Any] | None = None,
) -> ResolvedModelCatalog:
    resolved_root = project_root.resolve()
    if model_catalog_cfg is not None:
        raw_entries = model_catalog_cfg.get("models") or []
        if not isinstance(raw_entries, list):
            raise ConfigContractError("model_catalog.models must be a list")
        entries = [entry_from_payload(resolved_root, entry) for entry in raw_entries]
        return validate_catalog(
            catalog_from_entry_map(ensure_unique_entries(entries, source_name="resolved_model_catalog.models"))
        )

    registry_cfg = model_registry_cfg or {}
    if not isinstance(registry_cfg, dict):
        raise ConfigContractError("model_registry must be a mapping")

    registry_entries = [
        entry_from_payload(resolved_root, entry)
        for entry in load_registry_entries(resolved_root, registry_cfg.get("registry_file"))
    ]

    extra_entries = registry_cfg.get("extra_models") or []
    if not isinstance(extra_entries, list):
        raise ConfigContractError("model_registry.extra_models must be a list")
    for entry in extra_entries:
        if not isinstance(entry, dict):
            raise ConfigContractError("each model_registry.extra_models entry must be a mapping")
    override_entries = [entry_from_payload(resolved_root, entry) for entry in extra_entries]

    return validate_catalog(
        merge_catalog_layers(
            base_entries=registry_entries,
            override_entries=override_entries,
            base_name="model_registry.registry_file",
            override_name="model_registry.extra_models",
        )
    )


def model_catalog_cache_key(*, kind: str, payload: dict[str, Any]) -> str:
    return f"{kind}:{json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)}"


@dataclass
class ModelCatalogResolver:
    project_root: Path
    _cache: dict[str, ResolvedModelCatalog] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.project_root = self.project_root.resolve()

    def from_model_registry_cfg(self, model_registry_cfg: dict[str, Any] | None) -> ResolvedModelCatalog:
        registry_cfg = ensure_mapping(model_registry_cfg, "model_registry")
        return self._resolve(
            kind="model_registry",
            payload=registry_cfg,
            builder=lambda: build_model_catalog(self.project_root, model_registry_cfg=registry_cfg),
        )

    def from_model_catalog_cfg(self, model_catalog_cfg: dict[str, Any] | None) -> ResolvedModelCatalog:
        catalog_cfg = ensure_mapping(model_catalog_cfg, "resolved_model_catalog")
        return self._resolve(
            kind="resolved_model_catalog",
            payload=catalog_cfg,
            builder=lambda: build_model_catalog(self.project_root, model_catalog_cfg=catalog_cfg),
        )

    def _resolve(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        builder: Any,
    ) -> ResolvedModelCatalog:
        cache_key = model_catalog_cache_key(kind=kind, payload=payload)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        catalog = builder()
        self._cache[cache_key] = catalog
        return catalog
