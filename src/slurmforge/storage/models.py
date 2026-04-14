from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..pipeline.config.api import StorageConfigSpec
    from ..pipeline.config.runtime import NotifyConfig
    from ..pipeline.planning import BatchIdentity, PlannedRun
    from ..pipeline.planning.contracts import PlanDiagnostic
    from ..pipeline.status.models import ExecutionStatus


@dataclass(frozen=True)
class MaterializedBatchBundle:
    """Atomic snapshot of a fully planned batch, passed to PlanningStore.persist_materialized_batch."""

    identity: BatchIdentity
    planned_runs: tuple[PlannedRun, ...]
    planning_diagnostics: tuple[PlanDiagnostic, ...]
    notify_cfg: NotifyConfig | None
    submit_dependencies: dict[str, list[str]]
    manifest_extras: dict[str, Any]
    storage_config: StorageConfigSpec

    def __post_init__(self) -> None:
        object.__setattr__(self, "planned_runs", tuple(self.planned_runs))
        object.__setattr__(self, "planning_diagnostics", tuple(self.planning_diagnostics))
        object.__setattr__(self, "submit_dependencies", copy.deepcopy(self.submit_dependencies or {}))
        object.__setattr__(self, "manifest_extras", copy.deepcopy(self.manifest_extras or {}))

    @property
    def batch_root(self):  # -> Path
        return self.identity.batch_root

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
    def total_runs(self) -> int:
        return len(self.planned_runs)


@dataclass(frozen=True)
class RunExecutionView:
    """Full view of a single run: planning info + optional execution state.

    Returned by ``BatchStorageHandle.list_batch_run_views``.  Every run in the
    batch gets one entry — including runs that have never been attempted
    (``latest_status`` will be ``None``).
    """

    run_id: str
    run_index: int
    group_index: int
    task_index: int
    run_dir_rel: str
    run_dir: str
    latest_result_dir: str
    latest_status: ExecutionStatus | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", str(self.run_id or ""))
        object.__setattr__(self, "run_index", int(self.run_index))
        object.__setattr__(self, "group_index", int(self.group_index))
        object.__setattr__(self, "task_index", int(self.task_index))
        object.__setattr__(self, "run_dir_rel", str(self.run_dir_rel or ""))
        object.__setattr__(self, "run_dir", str(self.run_dir or ""))
        object.__setattr__(self, "latest_result_dir", str(self.latest_result_dir or ""))


@dataclass(frozen=True)
class LatestAttemptRecord:
    """Pure execution data for one run's latest attempt.

    Returned by ``ExecutionStore.load_latest_attempts``.  Contains only
    execution-phase information — no planning fields.  The join with planning
    data happens in ``BatchStorageHandle.list_batch_run_views``.
    """

    run_id: str
    latest_result_dir: str
    latest_status: ExecutionStatus | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", str(self.run_id or ""))
        object.__setattr__(self, "latest_result_dir", str(self.latest_result_dir or ""))


# Keep backward-compatible alias until all callers migrate
RunAttemptView = RunExecutionView
