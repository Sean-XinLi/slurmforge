from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ...models import BatchSharedSpec
from ...normalize import normalize_dispatch, normalize_notify, normalize_resources
from ...utils import ensure_dict, ensure_path_segment, resolve_spec_project_root
from ...validation.authoring import normalize_authoring_sweep_spec
from ..output import normalize_output_config
from ..storage import normalize_storage_config
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
    # Only ``max_available_gpus`` is batch-scoped from the top-level
    # ``resources:`` block; everything else (max_gpus_per_job, auto_gpu,
    # estimator knobs) is run-scoped and belongs on each run's spec, which
    # may diverge from the top-level via sweep axes.  We normalize the full
    # resources block here strictly to leverage its validation, then project
    # out the single batch-scoped scalar.  ``dispatch`` is batch-scoped in
    # its entirety today, so we keep the full object.
    base_resources = normalize_resources(ensure_dict(cfg.get("resources"), "resources"))
    return BatchSharedSpec(
        project_root=project_root,
        config_path=config_path,
        project=project,
        experiment_name=experiment_name,
        output=normalize_output_config(ensure_dict(cfg.get("output"), "output"), config_path=config_path),
        notify=normalize_notify(cfg.get("notify")),
        max_available_gpus=int(base_resources.max_available_gpus),
        dispatch_cfg=normalize_dispatch(ensure_dict(cfg.get("dispatch"), "dispatch")),
        storage=normalize_storage_config(cfg.get("storage")),
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
