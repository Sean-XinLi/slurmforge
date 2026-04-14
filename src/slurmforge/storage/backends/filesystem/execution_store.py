from __future__ import annotations

from pathlib import Path
from typing import Any, TYPE_CHECKING

from ...execution.journal import (
    read_execution_status as _read_execution_status,
    read_latest_result_dir as _read_latest_result_dir,
    write_artifact_manifest as _write_artifact_manifest,
    write_attempt_result as _write_attempt_result,
    write_checkpoint_state as _write_checkpoint_state,
    write_execution_status as _write_execution_status,
    write_latest_result_dir as _write_latest_result_dir,
    write_train_outputs_manifest as _write_train_outputs_manifest,
)

if TYPE_CHECKING:
    from ....pipeline.checkpoints.models import CheckpointState
    from ....pipeline.status.models import AttemptResult, ExecutionStatus
    from ....pipeline.train_outputs.models import TrainOutputsManifest
    from ....storage.models import LatestAttemptRecord


class FileSystemExecutionStore:
    """ExecutionStore implementation backed entirely by the filesystem.

    All I/O goes through ``storage.execution.journal`` — not through
    ``pipeline.status.store`` or ``pipeline.checkpoints.store``.
    """

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def write_latest_result_dir(self, run_dir: Path, result_dir: Path) -> None:
        _write_latest_result_dir(run_dir, result_dir)

    def write_execution_status(self, result_dir: Path, status: ExecutionStatus) -> None:
        _write_execution_status(result_dir, status)

    def write_attempt_result(self, result_dir: Path, attempt: AttemptResult) -> None:
        _write_attempt_result(result_dir, attempt)

    def write_checkpoint_state(self, result_dir: Path, state: CheckpointState) -> None:
        _write_checkpoint_state(result_dir, state)

    def write_train_outputs_manifest(self, result_dir: Path, manifest: TrainOutputsManifest) -> None:
        _write_train_outputs_manifest(result_dir, manifest)

    def write_artifact_manifest(self, result_dir: Path, manifest: dict[str, Any]) -> None:
        _write_artifact_manifest(result_dir, manifest)

    # ------------------------------------------------------------------
    # Reconcile — no-op for filesystem
    # ------------------------------------------------------------------

    def reconcile_batch(self, batch_root: Path) -> None:
        pass

    # ------------------------------------------------------------------
    # Query — scan run dirs for latest attempt status
    # ------------------------------------------------------------------

    def load_latest_attempts(
        self,
        batch_root: Path,
        run_id_to_run_dir: dict[str, Path],
    ) -> dict[str, LatestAttemptRecord]:
        from ...models import LatestAttemptRecord

        results: dict[str, LatestAttemptRecord] = {}
        for run_id, run_dir in run_id_to_run_dir.items():
            if not run_dir.exists():
                continue
            latest_result_dir = _read_latest_result_dir(run_dir)
            if latest_result_dir is None or not latest_result_dir.is_dir():
                continue
            status = _read_execution_status(latest_result_dir)
            results[run_id] = LatestAttemptRecord(
                run_id=run_id,
                latest_result_dir=str(latest_result_dir),
                latest_status=status,
            )
        return results
