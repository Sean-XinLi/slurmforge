from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..pipeline.checkpoints.models import CheckpointState
    from ..pipeline.records.models.run_plan import RunPlan
    from ..pipeline.records.models.run_snapshot import RunSnapshot
    from ..pipeline.status.models import AttemptResult, ExecutionStatus
    from ..pipeline.train_outputs.models import TrainOutputsManifest
    from .models import LatestAttemptRecord, MaterializedBatchBundle


@runtime_checkable
class PlanningStore(Protocol):
    """Unified interface for reading and writing batch planning data."""

    def persist_materialized_batch(
        self,
        bundle: MaterializedBatchBundle,
    ) -> tuple[dict[str, Any], ...]: ...

    def load_batch_run_plans(self, batch_root: Path) -> tuple[RunPlan, ...]: ...

    def load_run_snapshot(self, batch_root: Path, run_id: str) -> RunSnapshot | None: ...

    def load_plan_for_array_task(
        self, batch_root: Path, group_index: int, task_index: int,
    ) -> RunPlan | None: ...


@runtime_checkable
class ExecutionStore(Protocol):
    """Unified interface for runtime execution data.

    All write methods produce files regardless of engine (NFS safety).
    Parameters are semantic (``run_dir``, ``result_dir``), not physical paths.
    """

    def write_latest_result_dir(self, run_dir: Path, result_dir: Path) -> None: ...

    def write_execution_status(self, result_dir: Path, status: ExecutionStatus) -> None: ...

    def write_attempt_result(self, result_dir: Path, attempt: AttemptResult) -> None: ...

    def write_checkpoint_state(self, result_dir: Path, state: CheckpointState) -> None: ...

    def write_train_outputs_manifest(self, result_dir: Path, manifest: TrainOutputsManifest) -> None: ...

    def write_artifact_manifest(self, result_dir: Path, manifest: dict[str, Any]) -> None: ...

    def reconcile_batch(self, batch_root: Path) -> None: ...

    def load_latest_attempts(
        self,
        batch_root: Path,
        run_id_to_run_dir: dict[str, Path],
    ) -> dict[str, LatestAttemptRecord]:
        """Return attempted runs as run_id → LatestAttemptRecord.

        ``run_id_to_run_dir`` is provided by the caller (BatchStorageHandle)
        so that ExecutionStore does not need to load planning data itself.
        """
        ...
