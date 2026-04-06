from __future__ import annotations

from typing import Any

from ...config.api import ExperimentSpec
from ..identity import BatchIdentity
from ..replay_builder import build_run_replay_spec
from .assembly import RunPlanAssembly
from .identity import resolve_run_identity
from .stages import build_run_stages


def build_run_plan(
    spec: ExperimentSpec,
    *,
    run_index: int,
    total_runs: int,
    identity: BatchIdentity,
    generated_by,
    sweep_case_name: str | None = None,
    sweep_assignments: dict[str, Any] | None = None,
    replay_source_batch_root: str | None = None,
    replay_source_run_id: str | None = None,
    replay_source_record_path: str | None = None,
) -> RunPlanAssembly:
    from ...records.batch_paths import batch_relative_path
    from ...records.models.dispatch import DispatchInfo
    from ...records.models.run_plan import RunPlan

    built = build_run_stages(
        spec,
        project_root=identity.project_root,
        batch_root=identity.batch_root,
        run_index=run_index,
    )
    resolved = resolve_run_identity(
        spec,
        train_mode=built.prepared.train_mode,
        model_name=built.prepared.model_spec.name,
        project_root=identity.project_root,
        batch_root=identity.batch_root,
        run_index=run_index,
    )
    replay_spec = build_run_replay_spec(
        spec,
        project_root=identity.project_root,
        replay_source_batch_root=replay_source_batch_root,
        replay_source_run_id=replay_source_run_id,
        replay_source_record_path=replay_source_record_path,
    )
    plan = RunPlan(
        run_index=run_index,
        total_runs=total_runs,
        run_id=resolved.run_id,
        project=spec.project,
        experiment_name=spec.experiment_name,
        model_name=resolved.model_name,
        train_mode=built.prepared.train_mode,
        train_stage=built.train_stage,
        eval_stage=built.eval_stage,
        eval_train_outputs=spec.eval.train_outputs,
        cluster=(
            built.train_stage.cluster_cfg
            if built.train_stage.cluster_cfg is not None
            else built.prepared.cluster_cfg
        ),
        env=built.prepared.env_cfg,
        run_dir="",
        run_dir_rel=batch_relative_path(identity.batch_root, resolved.run_dir),
        dispatch=DispatchInfo(),
        artifacts=built.prepared.artifacts_cfg,
        sweep_case_name=sweep_case_name,
        sweep_assignments={} if sweep_assignments is None else dict(sweep_assignments),
        generated_by=generated_by,
        planning_diagnostics=tuple(built.train_stage.diagnostics)
        + (() if built.eval_stage is None else tuple(built.eval_stage.diagnostics)),
    )
    return RunPlanAssembly(plan=plan, replay_spec=replay_spec)
