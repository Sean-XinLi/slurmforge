from __future__ import annotations

from dataclasses import dataclass
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
class BatchFirstWinsState:
    """Batch-scoped fields that use first-wins consistency.

    These four travel together throughout the compile lifecycle: the
    authoring flow seeds them from ``BatchSharedSpec``; the replay flow
    seeds them from the first accepted spec; every subsequent spec must
    agree (mismatch → user-facing batch diagnostic, not
    ``InternalCompilerError``).  The set matches the ``first_wins`` column
    of the field contract registry — ``tests/test_contracts.py`` enforces
    that alignment.
    """

    identity: BatchIdentity
    notify_cfg: NotifyConfig
    submit_dependencies: dict[str, list[str]]
    storage_config: StorageConfigSpec


@dataclass(frozen=True)
class CompileState:
    """Running compile-time state, built up as each spec is accepted.

    - ``first_wins`` holds the batch-scoped fields that must agree across
      all accepted specs (identity / notify / submit_dependencies /
      storage).  ``None`` until the first spec has been accepted.
    - ``max_available_gpus_candidates`` and ``dispatch_policy_candidates``
      accumulate one entry per accepted spec.  The final batch-scoped
      value is resolved to a single winner (or a ConfigContractError) in
      ``build_materialized_report`` via ``resolve_batch_scope_unique``.

    We never store a full ``ResourcesConfig`` at batch level — most of its
    fields are run-scoped and must diverge freely across sweep / replay
    runs; only ``max_available_gpus`` is batch-scoped, and it lives in
    ``max_available_gpus_candidates`` until it's collapsed to a scalar.
    """

    first_wins: BatchFirstWinsState | None = None
    batch_diagnostics: tuple[PlanDiagnostic, ...] = ()
    max_available_gpus_candidates: tuple[int, ...] = ()
    dispatch_policy_candidates: tuple[str, ...] = ()

    # ------------------------------------------------------------------
    # Convenience accessors.  Callers that only need one sub-field of
    # ``first_wins`` can use these to avoid ``state.first_wins and state.first_wins.x``
    # ceremony.  They return ``None`` when no spec has been accepted yet.
    # ------------------------------------------------------------------

    @property
    def identity(self) -> BatchIdentity | None:
        return None if self.first_wins is None else self.first_wins.identity

    @property
    def notify_cfg(self) -> NotifyConfig | None:
        return None if self.first_wins is None else self.first_wins.notify_cfg

    @property
    def submit_dependencies(self) -> dict[str, list[str]] | None:
        return None if self.first_wins is None else self.first_wins.submit_dependencies

    @property
    def storage_config(self) -> StorageConfigSpec:
        # Match prior default when no spec is accepted yet.
        return StorageConfigSpec() if self.first_wins is None else self.first_wins.storage_config
