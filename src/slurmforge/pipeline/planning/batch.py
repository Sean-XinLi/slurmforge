from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..config.api import StorageConfigSpec
from ..config.runtime import DispatchConfig, NotifyConfig
from ..config.runtime.defaults import DEFAULT_RESOURCES
from .batch_validator import validate_planned_batch_runs
from .identity import BatchIdentity

if TYPE_CHECKING:
    from ..records.models.run_plan import RunPlan
    from ..records.models.run_snapshot import RunSnapshot
    from .gpu_budget import GpuBudgetPlan


@dataclass(frozen=True)
class PlannedRun:
    plan: "RunPlan"
    snapshot: "RunSnapshot"


@dataclass(frozen=True)
class PlannedBatch:
    """Final planned batch ready for materialization.

    Only batch-scoped GPU fields live here: ``max_available_gpus`` (int)
    and the resolved ``dispatch_cfg``.  Per-run resource knobs
    (max_gpus_per_job, auto_gpu, estimator profile, ...) remain on each
    ``PlannedRun`` via its ``train_stage`` and are not accessible at
    batch level.
    """

    identity: BatchIdentity
    planned_runs: tuple[PlannedRun, ...]
    notify_cfg: NotifyConfig | None = None
    submit_dependencies: dict[str, list[str]] = field(default_factory=dict)
    manifest_extras: dict[str, Any] = field(default_factory=dict)
    storage_config: StorageConfigSpec = field(default_factory=StorageConfigSpec)
    max_available_gpus: int = int(DEFAULT_RESOURCES["max_available_gpus"])
    dispatch_cfg: DispatchConfig = field(default_factory=DispatchConfig)
    gpu_budget_plan: "GpuBudgetPlan | None" = None

    def __post_init__(self) -> None:
        identity = self.identity
        runs = tuple(self.planned_runs)
        validate_planned_batch_runs(runs, identity=identity)
        object.__setattr__(self, "identity", identity)
        object.__setattr__(self, "planned_runs", runs)
        object.__setattr__(self, "submit_dependencies", copy.deepcopy(self.submit_dependencies or {}))
        object.__setattr__(self, "manifest_extras", copy.deepcopy(self.manifest_extras or {}))

    @property
    def total_runs(self) -> int:
        return len(self.planned_runs)

    @property
    def project(self) -> str:
        return self.identity.project

    @property
    def experiment_name(self) -> str:
        return self.identity.experiment_name

    @property
    def batch_name(self) -> str:
        return self.identity.batch_name

    @property
    def batch_root(self) -> Path:
        return self.identity.batch_root

    @property
    def sbatch_dir(self) -> Path:
        return self.identity.sbatch_dir
