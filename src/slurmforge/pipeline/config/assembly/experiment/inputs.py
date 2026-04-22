from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...mode_detection import detect_run_mode
from ...models import BatchSharedSpec
from ...utils import ensure_dict, ensure_path_segment


@dataclass(frozen=True)
class ExperimentSectionInputs:
    config_path: Path | str
    project: str
    experiment_name: str
    run_mode: str
    model_cfg_raw: Any
    run_cfg: dict[str, Any]
    eval_cfg: dict[str, Any]
    launcher_cfg_raw: dict[str, Any]
    cluster_cfg_raw: dict[str, Any]
    env_cfg_raw: dict[str, Any]
    resources_cfg_raw: dict[str, Any]
    dispatch_cfg_raw: dict[str, Any]
    artifacts_cfg_raw: dict[str, Any]
    output_cfg_raw: dict[str, Any]
    notify_cfg_raw: Any
    validation_cfg_raw: Any
    storage_cfg_raw: Any


def gather_experiment_section_inputs(
    cfg: dict[str, Any],
    *,
    config_path: Path | str,
    batch_shared: BatchSharedSpec | None = None,
) -> ExperimentSectionInputs:
    run_cfg = ensure_dict(cfg.get("run"), "run")
    if batch_shared is None:
        project = ensure_path_segment(cfg.get("project"), name=f"{config_path}: project")
        experiment_name = ensure_path_segment(cfg.get("experiment_name"), name=f"{config_path}: experiment_name")
    else:
        project = batch_shared.project
        experiment_name = batch_shared.experiment_name

    return ExperimentSectionInputs(
        config_path=config_path,
        project=project,
        experiment_name=experiment_name,
        run_mode=detect_run_mode(run_cfg),
        model_cfg_raw=cfg.get("model"),
        run_cfg=run_cfg,
        eval_cfg=ensure_dict(cfg.get("eval"), "eval"),
        launcher_cfg_raw=ensure_dict(cfg.get("launcher"), "launcher"),
        cluster_cfg_raw=ensure_dict(cfg.get("cluster"), "cluster"),
        env_cfg_raw=ensure_dict(cfg.get("env"), "env"),
        resources_cfg_raw=ensure_dict(cfg.get("resources"), "resources"),
        dispatch_cfg_raw=ensure_dict(cfg.get("dispatch"), "dispatch"),
        artifacts_cfg_raw=ensure_dict(cfg.get("artifacts"), "artifacts"),
        output_cfg_raw=ensure_dict(cfg.get("output"), "output"),
        notify_cfg_raw=cfg.get("notify"),
        validation_cfg_raw=cfg.get("validation"),
        storage_cfg_raw=cfg.get("storage"),
    )
