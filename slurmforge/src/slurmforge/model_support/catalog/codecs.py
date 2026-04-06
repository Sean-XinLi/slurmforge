from __future__ import annotations

from typing import Any

from .models import ModelCatalogEntry, ResolvedModelCatalog


def serialize_model_catalog_entry(entry: ModelCatalogEntry) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": entry.name,
        "script": entry.script,
        "ddp_supported": bool(entry.ddp_supported),
        "ddp_required": bool(entry.ddp_required),
        "estimator_profile": entry.estimator_profile,
    }
    if entry.yaml is not None:
        payload["yaml"] = entry.yaml
    return payload


def serialize_resolved_model_catalog(catalog: ResolvedModelCatalog) -> dict[str, Any]:
    return {
        "models": [serialize_model_catalog_entry(entry) for entry in catalog.entries],
    }
