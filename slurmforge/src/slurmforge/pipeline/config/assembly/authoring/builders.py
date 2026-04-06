from __future__ import annotations

from pathlib import Path

from .....errors import ConfigContractError
from .....model_support.catalog import ModelCatalogResolver
from ...models import BatchRunSpec, BatchSpec, ExperimentSpec
from ..spec_builder import materialize_authoring_experiment_spec
from .expansion import iter_authoring_materialized_specs
from .shared import prepare_authoring_batch_input


def build_experiment_spec(
    cfg: dict,
    config_path: Path,
    *,
    project_root: Path | None = None,
) -> ExperimentSpec:
    prepared = prepare_authoring_batch_input(
        cfg,
        config_path=config_path,
        project_root=project_root,
    )
    if prepared.sweep_spec.shared_axes or any(case.set_values or case.axes for case in prepared.sweep_spec.cases):
        raise ConfigContractError(
            f"{config_path}: build_experiment_spec does not accept sweep configs; use pipeline.compiler.compile_source"
        )
    model_catalog_resolver = ModelCatalogResolver(prepared.project_root)
    return materialize_authoring_experiment_spec(
        prepared.base_cfg,
        config_path=config_path,
        project_root=prepared.project_root,
        batch_shared=prepared.shared,
        model_catalog_resolver=model_catalog_resolver,
    )


def build_batch_spec(
    cfg: dict,
    config_path: Path,
    *,
    project_root: Path | None = None,
) -> BatchSpec:
    prepared = prepare_authoring_batch_input(
        cfg,
        config_path=config_path,
        project_root=project_root,
    )
    runs: list[BatchRunSpec] = []
    for expansion, spec in iter_authoring_materialized_specs(
        base_cfg=prepared.base_cfg,
        sweep_spec=prepared.sweep_spec,
        config_path=config_path,
        project_root=prepared.project_root,
        shared=prepared.shared,
    ):
        runs.append(
            BatchRunSpec(
                spec=spec,
                case_name=expansion.case_name,
                assignments=expansion.assignments,
            )
        )
    if not runs:
        raise ConfigContractError(f"{config_path}: sweep expansion produced no runs")

    return BatchSpec(
        shared=prepared.shared,
        runs=tuple(runs),
    )
