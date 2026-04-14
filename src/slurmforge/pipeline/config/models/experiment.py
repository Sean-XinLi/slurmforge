from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ....model_support.catalog import ResolvedModelCatalog
from ..runtime import (
    ArtifactsConfig,
    ClusterConfig,
    EnvConfig,
    LauncherConfig,
    NotifyConfig,
    ResourcesConfig,
    ValidationConfig,
)
from .eval import EvalConfigSpec
from .model import ModelConfigSpec
from .output import OutputConfigSpec
from .run import RunConfigSpec
from .runtime import PlanningHints
from .storage import StorageConfigSpec


@dataclass(frozen=True)
class ExperimentSpec:
    project: str
    experiment_name: str
    run: RunConfigSpec
    launcher: LauncherConfig
    cluster: ClusterConfig
    env: EnvConfig
    resources: ResourcesConfig
    artifacts: ArtifactsConfig
    eval: EvalConfigSpec
    output: OutputConfigSpec
    notify: NotifyConfig
    validation: ValidationConfig
    storage: StorageConfigSpec = field(default_factory=StorageConfigSpec)
    model: ModelConfigSpec | None = None
    model_catalog: ResolvedModelCatalog = field(default_factory=ResolvedModelCatalog)
    hints: PlanningHints = field(default_factory=PlanningHints)


@dataclass(frozen=True)
class BatchSharedSpec:
    project_root: Path
    config_path: Path
    project: str
    experiment_name: str
    output: OutputConfigSpec
    notify: NotifyConfig
    storage: StorageConfigSpec = field(default_factory=StorageConfigSpec)


@dataclass(frozen=True)
class BatchRunSpec:
    spec: ExperimentSpec
    case_name: str | None = None
    assignments: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class BatchSpec:
    shared: BatchSharedSpec
    runs: tuple[BatchRunSpec, ...]

    @property
    def total_runs(self) -> int:
        return len(self.runs)

    @property
    def project_root(self) -> Path:
        return self.shared.project_root

    @property
    def config_path(self) -> Path:
        return self.shared.config_path

    @property
    def project(self) -> str:
        return self.shared.project

    @property
    def experiment_name(self) -> str:
        return self.shared.experiment_name

    @property
    def output(self) -> OutputConfigSpec:
        return self.shared.output

    @property
    def notify(self) -> NotifyConfig:
        return self.shared.notify
