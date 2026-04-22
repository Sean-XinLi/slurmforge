from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ....model_support.catalog import ResolvedModelCatalog
from ..runtime import (
    ArtifactsConfig,
    ClusterConfig,
    DispatchConfig,
    EnvConfig,
    LauncherConfig,
    NotifyConfig,
    ResourcesConfig,
    ValidationConfig,
)
from ..runtime.defaults import DEFAULT_RESOURCES
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
    dispatch: DispatchConfig = field(default_factory=DispatchConfig)
    model: ModelConfigSpec | None = None
    model_catalog: ResolvedModelCatalog = field(default_factory=ResolvedModelCatalog)
    hints: PlanningHints = field(default_factory=PlanningHints)


@dataclass(frozen=True)
class BatchSharedSpec:
    """Batch-wide configuration shared by every run in the batch.

    Every field here is batch-scoped per the contract registry
    (``pipeline/config/contracts/fields.py``).  Run-scoped fields like
    ``resources.max_gpus_per_job`` or ``resources.auto_gpu`` deliberately
    do NOT live here — they belong on each ``ExperimentSpec`` so that
    sweep axes and replay-level per-run variation stay meaningful.

    ``resources`` as a whole is NOT present because most of its fields are
    run-scoped.  Only the single batch-scoped knob ``max_available_gpus``
    is projected out.  ``dispatch`` contains only one batch-scoped field
    today (``group_overflow_policy``), so we carry the whole
    ``DispatchConfig`` for future-proofing.
    """

    project_root: Path
    config_path: Path
    project: str
    experiment_name: str
    output: OutputConfigSpec
    notify: NotifyConfig
    max_available_gpus: int = int(DEFAULT_RESOURCES["max_available_gpus"])
    dispatch_cfg: DispatchConfig = field(default_factory=DispatchConfig)
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
