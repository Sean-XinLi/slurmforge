from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ...models import BatchSharedSpec
from ...normalize import normalize_notify
from ...utils import ensure_dict, ensure_path_segment, resolve_spec_project_root
from ...validation.authoring import normalize_authoring_sweep_spec
from ..output import normalize_output_config
from .models import PreparedAuthoringBatchInput


def cfg_without_sweep(cfg: dict[str, Any]) -> dict[str, Any]:
    materialized = copy.deepcopy(cfg)
    materialized.pop("sweep", None)
    return materialized


def build_batch_shared_spec(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path,
) -> BatchSharedSpec:
    project = ensure_path_segment(cfg.get("project"), name=f"{config_path}: project")
    experiment_name = ensure_path_segment(cfg.get("experiment_name"), name=f"{config_path}: experiment_name")
    return BatchSharedSpec(
        project_root=project_root,
        config_path=config_path,
        project=project,
        experiment_name=experiment_name,
        output=normalize_output_config(ensure_dict(cfg.get("output"), "output"), config_path=config_path),
        notify=normalize_notify(cfg.get("notify")),
    )


def prepare_authoring_batch_input(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path | None = None,
) -> PreparedAuthoringBatchInput:
    resolved_project_root = resolve_spec_project_root(config_path, project_root)
    sweep_spec = normalize_authoring_sweep_spec(cfg, config_path=config_path)
    base_cfg = cfg_without_sweep(cfg)
    shared = build_batch_shared_spec(
        base_cfg,
        config_path=config_path,
        project_root=resolved_project_root,
    )
    return PreparedAuthoringBatchInput(
        project_root=resolved_project_root,
        sweep_spec=sweep_spec,
        base_cfg=base_cfg,
        shared=shared,
    )
