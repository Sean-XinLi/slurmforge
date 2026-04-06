from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ModelSpec:
    name: str
    script: Path | None
    yaml_path: Path | None
    ddp_supported: bool = True
    ddp_required: bool = False
    estimator_profile: str = "default"


@dataclass(frozen=True)
class ModelCatalogEntry:
    name: str
    script: str
    yaml: str | None = None
    ddp_supported: bool = True
    ddp_required: bool = False
    estimator_profile: str = "default"


@dataclass(frozen=True)
class ResolvedModelCatalog:
    entries: tuple[ModelCatalogEntry, ...] = field(default_factory=tuple)
