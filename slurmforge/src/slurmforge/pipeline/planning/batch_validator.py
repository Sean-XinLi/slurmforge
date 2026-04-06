from __future__ import annotations

from typing import TYPE_CHECKING

from ...errors import PlanningError
from .identity import BatchIdentity

if TYPE_CHECKING:
    from .batch import PlannedRun


def validate_planned_batch_runs(
    planned_runs: tuple["PlannedRun", ...],
    *,
    identity: BatchIdentity,
) -> None:
    from ..records.batch_paths import batch_relative_path

    if not planned_runs:
        raise PlanningError("PlannedBatch requires at least one planned run")

    total_runs = len(planned_runs)

    for expected_index, planned_run in enumerate(planned_runs, start=1):
        plan = planned_run.plan
        snapshot = planned_run.snapshot
        if plan.run_index != expected_index or snapshot.run_index != expected_index:
            raise PlanningError("PlannedBatch run indices must be sequential and start at 1")
        if plan.total_runs != total_runs or snapshot.total_runs != total_runs:
            raise PlanningError("PlannedBatch total_runs must match the actual number of planned runs")
        if plan.project != identity.project or snapshot.project != identity.project:
            raise PlanningError("PlannedBatch runs must match BatchIdentity.project")
        if plan.experiment_name != identity.experiment_name or snapshot.experiment_name != identity.experiment_name:
            raise PlanningError("PlannedBatch runs must match BatchIdentity.experiment_name")
        if plan.run_id != snapshot.run_id:
            raise PlanningError("PlannedBatch plan/snapshot run_id must match")
        expected_run_dir_rel = batch_relative_path(
            identity.batch_root,
            identity.batch_root / "runs" / f"run_{expected_index:03d}_{plan.run_id}",
        )
        if plan.run_dir_rel != expected_run_dir_rel:
            raise PlanningError("PlannedBatch runs must be planned against the canonical BatchIdentity.batch_root")
