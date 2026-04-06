from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

from .....errors import ConfigContractError
from .....model_support.catalog import ModelCatalogResolver
from .....sweep import SweepExpansion, SweepSpec, iter_sweep_expansions, materialize_override_assignments
from ...models import BatchSharedSpec, ExperimentSpec
from ..spec_builder import materialize_authoring_experiment_spec


def iter_authoring_materialized_specs(
    *,
    base_cfg: dict[str, Any],
    sweep_spec: SweepSpec,
    config_path: Path,
    project_root: Path,
    shared: BatchSharedSpec,
) -> Iterator[tuple[SweepExpansion, ExperimentSpec]]:
    model_catalog_resolver = ModelCatalogResolver(project_root)
    for expansion in iter_sweep_expansions(sweep_spec):
        run_cfg = materialize_override_assignments(base_cfg, expansion.assignments)
        spec = materialize_authoring_experiment_spec(
            run_cfg,
            config_path=config_path,
            project_root=project_root,
            batch_shared=shared,
            model_catalog_resolver=model_catalog_resolver,
        )
        if spec.project != shared.project or spec.experiment_name != shared.experiment_name:
            raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch identity")
        if spec.output != shared.output:
            raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch output configuration")
        if spec.notify != shared.notify:
            raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch notify configuration")
        yield expansion, spec


def iter_authoring_static_validation_cfgs(
    *,
    base_cfg: dict[str, Any],
    sweep_spec: SweepSpec,
) -> Iterator[tuple[str, dict[str, Any]]]:
    yield ("base", base_cfg)

    for path, values in sweep_spec.shared_axes:
        for idx, value in enumerate(values):
            yield (
                f"sweep.shared_axes.{path}[{idx}]",
                materialize_override_assignments(base_cfg, ((path, value),)),
            )

    for case_idx, case in enumerate(sweep_spec.cases):
        if case.set_values:
            case_cfg = materialize_override_assignments(base_cfg, case.set_values)
            yield (f"sweep.cases[{case_idx}].set", case_cfg)
        else:
            case_cfg = base_cfg

        for path, values in case.axes:
            for value_idx, value in enumerate(values):
                yield (
                    f"sweep.cases[{case_idx}].axes.{path}[{value_idx}]",
                    materialize_override_assignments(case_cfg, ((path, value),)),
                )
