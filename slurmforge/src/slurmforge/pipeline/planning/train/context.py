from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ....model_support.gpu_estimator import GpuEstimate
from ....model_support.catalog import ModelSpec
from ...config.api import EvalConfigSpec, ExperimentSpec, RunConfigSpec
from ...config.runtime import (
    ArtifactsConfig,
    ClusterConfig,
    EnvConfig,
    LauncherConfig,
    ResourcesConfig,
    ValidationConfig,
)


@dataclass
class TrainContext:
    run_index: int
    project_root: Path
    run_spec: RunConfigSpec
    model_spec: ModelSpec
    launcher_cfg: LauncherConfig
    cluster_cfg: ClusterConfig
    launcher_nproc_per_node_explicit: bool
    cluster_nodes_explicit: bool
    cluster_gpus_per_node_explicit: bool
    resources_cfg: ResourcesConfig
    run_args: dict[str, Any]
    model_overrides: dict[str, Any]
    estimate: GpuEstimate
    validation_cfg: ValidationConfig


@dataclass
class PreparedTrainPlan:
    spec: ExperimentSpec
    project_root: Path
    train_mode: str
    model_spec: ModelSpec
    launcher_cfg: LauncherConfig
    cluster_cfg: ClusterConfig
    cluster_nodes_explicit: bool
    cluster_gpus_per_node_explicit: bool
    launcher_nproc_per_node_explicit: bool
    env_cfg: EnvConfig
    resources_cfg: ResourcesConfig
    artifacts_cfg: ArtifactsConfig
    validation_cfg: ValidationConfig
    eval_spec: EvalConfigSpec
    run_args: dict[str, Any]
    model_overrides: dict[str, Any]
