from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ...model_support.catalog import ModelCatalogResolver
from ..config.api import StorageConfigSpec
from ..config.runtime import NotifyConfig
from ..planning import BatchIdentity
from ..planning.contracts import PlanDiagnostic
from .reports.models import SourceCollectionReport


@dataclass(frozen=True)
class AuthoringCollectedState:
    config_path: Path
    project_root: Path
    shared: Any


@dataclass(frozen=True)
class AuthoringMaterializedState:
    config_path: Path
    project_root: Path
    shared: Any
    model_catalog_resolver: ModelCatalogResolver
    default_batch_name: str


@dataclass(frozen=True)
class ReplayMaterializedState:
    project_root_override: Path | None
    project_root: Path
    cli_overrides: tuple[str, ...]
    parsed_overrides: tuple[tuple[str, Any], ...]
    default_batch_name: str
    manifest_context_key: str | None


@dataclass(frozen=True)
class CollectedSourceBundle:
    report: SourceCollectionReport
    payload: AuthoringCollectedState | None = None


@dataclass(frozen=True)
class MaterializedSourceBundle:
    report: SourceCollectionReport
    context: AuthoringMaterializedState | ReplayMaterializedState | None
    batch_diagnostics: tuple[PlanDiagnostic, ...]
    manifest_extras: dict[str, Any]


@dataclass(frozen=True)
class CompileState:
    identity: BatchIdentity | None = None
    notify_cfg: NotifyConfig | None = None
    submit_dependencies: dict[str, list[str]] | None = None
    batch_diagnostics: tuple[PlanDiagnostic, ...] = ()
    storage_config: StorageConfigSpec = field(default_factory=StorageConfigSpec)
