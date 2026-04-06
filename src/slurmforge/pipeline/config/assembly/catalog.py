from __future__ import annotations

from pathlib import Path
from typing import Any

from ....model_support.catalog import ModelCatalogResolver, ResolvedModelCatalog
from ..constants import REPLAY_MODEL_CATALOG_KEY
from ..utils import ensure_dict


def resolve_authoring_model_catalog(
    cfg: dict[str, Any],
    *,
    project_root: Path,
    resolver: ModelCatalogResolver | None = None,
) -> ResolvedModelCatalog:
    catalog_resolver = resolver or ModelCatalogResolver(project_root)
    return catalog_resolver.from_model_registry_cfg(ensure_dict(cfg.get("model_registry"), "model_registry"))


def resolve_replay_model_catalog(
    cfg: dict[str, Any],
    *,
    project_root: Path,
    resolver: ModelCatalogResolver | None = None,
) -> ResolvedModelCatalog:
    catalog_resolver = resolver or ModelCatalogResolver(project_root)
    return catalog_resolver.from_model_catalog_cfg(
        ensure_dict(cfg.get(REPLAY_MODEL_CATALOG_KEY), REPLAY_MODEL_CATALOG_KEY)
    )
