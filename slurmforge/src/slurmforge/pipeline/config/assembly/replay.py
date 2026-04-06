from __future__ import annotations

import copy
from pathlib import Path

from ....model_support.catalog import ModelCatalogResolver
from ..models import ExperimentSpec
from ..validation.replay import validate_replay_config
from .spec_builder import materialize_replay_experiment_spec


def build_replay_experiment_spec(
    replay_cfg: dict,
    *,
    project_root: Path,
    config_path: Path | None = None,
    config_label: str | None = None,
) -> ExperimentSpec:
    config_ref, resolved_project_root = validate_replay_config(
        replay_cfg,
        config_path=config_path,
        config_label=config_label,
        project_root=project_root,
    )
    model_catalog_resolver = ModelCatalogResolver(resolved_project_root)
    return materialize_replay_experiment_spec(
        copy.deepcopy(replay_cfg),
        config_path=config_ref,
        project_root=resolved_project_root,
        model_catalog_resolver=model_catalog_resolver,
    )
