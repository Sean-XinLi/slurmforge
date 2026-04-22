from __future__ import annotations

from pathlib import Path
from typing import Any

from ....model_support.catalog import ModelCatalogResolver, ResolvedModelCatalog
from ..models import BatchSharedSpec, ExperimentSpec
from ..utils import resolve_spec_project_root
from .catalog import resolve_authoring_model_catalog, resolve_replay_model_catalog
from .experiment import normalize_experiment_contract


def materialize_resolved_experiment_spec(
    cfg: dict[str, Any],
    *,
    config_path: Path | str,
    model_catalog: ResolvedModelCatalog,
    batch_shared: BatchSharedSpec | None = None,
) -> ExperimentSpec:
    contract = normalize_experiment_contract(
        cfg,
        config_path=config_path,
        batch_shared=batch_shared,
    )
    return ExperimentSpec(
        project=contract.project,
        experiment_name=contract.experiment_name,
        model=contract.model,
        model_catalog=model_catalog,
        run=contract.run,
        launcher=contract.launcher,
        cluster=contract.cluster,
        env=contract.env,
        resources=contract.resources,
        dispatch=contract.dispatch,
        artifacts=contract.artifacts,
        eval=contract.eval,
        output=contract.output,
        notify=contract.notify,
        validation=contract.validation,
        storage=contract.storage,
        hints=contract.hints,
    )


def materialize_authoring_experiment_spec(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path,
    batch_shared: BatchSharedSpec | None = None,
    model_catalog_resolver: ModelCatalogResolver | None = None,
) -> ExperimentSpec:
    resolved_project_root = resolve_spec_project_root(config_path, project_root)
    return materialize_resolved_experiment_spec(
        cfg,
        config_path=config_path,
        model_catalog=resolve_authoring_model_catalog(
            cfg,
            project_root=resolved_project_root,
            resolver=model_catalog_resolver,
        ),
        batch_shared=batch_shared,
    )


def materialize_replay_experiment_spec(
    cfg: dict[str, Any],
    *,
    config_path: Path | str,
    project_root: Path,
    model_catalog_resolver: ModelCatalogResolver | None = None,
) -> ExperimentSpec:
    resolved_project_root = resolve_spec_project_root(None if isinstance(config_path, str) else config_path, project_root)
    return materialize_resolved_experiment_spec(
        cfg,
        config_path=config_path,
        model_catalog=resolve_replay_model_catalog(
            cfg,
            project_root=resolved_project_root,
            resolver=model_catalog_resolver,
        ),
    )
