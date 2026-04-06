from __future__ import annotations

from typing import Any

from ....identity import __version__
from ...config.api import ExperimentSpec
from ...records.models.metadata import GeneratedBy
from ..batch import PlannedRun
from ..identity import BatchIdentity
from ..snapshot_builder import build_run_snapshot
from .plan_factory import build_run_plan


def build_planned_run(
    spec: ExperimentSpec,
    *,
    run_index: int,
    total_runs: int,
    identity: BatchIdentity,
    sweep_case_name: str | None = None,
    sweep_assignments: dict[str, Any] | None = None,
    replay_source_batch_root: str | None = None,
    replay_source_run_id: str | None = None,
    replay_source_record_path: str | None = None,
) -> PlannedRun:
    generated_by = GeneratedBy(version=__version__)
    assembly = build_run_plan(
        spec,
        run_index=run_index,
        total_runs=total_runs,
        identity=identity,
        generated_by=generated_by,
        sweep_case_name=sweep_case_name,
        sweep_assignments=sweep_assignments,
        replay_source_batch_root=replay_source_batch_root,
        replay_source_run_id=replay_source_run_id,
        replay_source_record_path=replay_source_record_path,
    )
    snapshot = build_run_snapshot(
        run_index=run_index,
        total_runs=total_runs,
        run_id=assembly.plan.run_id,
        generated_by=generated_by,
        project=spec.project,
        experiment_name=spec.experiment_name,
        model_name=assembly.plan.model_name,
        train_mode=assembly.plan.train_mode,
        replay_spec=assembly.replay_spec,
        sweep_case_name=sweep_case_name,
        sweep_assignments=sweep_assignments,
    )
    return PlannedRun(plan=assembly.plan, snapshot=snapshot)
