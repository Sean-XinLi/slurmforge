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
    """Running compile-time state, built up as each spec is accepted.

    ``max_available_gpus_candidates`` and ``dispatch_policy_candidates``
    accumulate one entry per accepted spec.  The final batch-scoped value
    is resolved to a single winner (or a ConfigContractError) in
    ``build_materialized_report`` via ``resolve_batch_scope_unique``.

    We never store a full ``ResourcesConfig`` at batch level — most of its
    fields are run-scoped and must diverge freely across sweep / replay
    runs; only ``max_available_gpus`` is batch-scoped, and it lives in
    ``max_available_gpus_candidates`` until it's collapsed to a scalar.
    """

    identity: BatchIdentity | None = None
    notify_cfg: NotifyConfig | None = None
    submit_dependencies: dict[str, list[str]] | None = None
    batch_diagnostics: tuple[PlanDiagnostic, ...] = ()
    storage_config: StorageConfigSpec = field(default_factory=StorageConfigSpec)
    max_available_gpus_candidates: tuple[int, ...] = ()
    dispatch_policy_candidates: tuple[str, ...] = ()
