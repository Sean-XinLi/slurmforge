from __future__ import annotations

from typing import Any

from ...errors import InternalCompilerError
from ..planning import BatchIdentity, PlannedRun, build_planned_run
from ..planning.contracts import PlanDiagnostic
from ..sources.inference import infer_model_name_from_cfg, infer_train_mode_from_cfg
from ..sources.models import FailedCompiledRun, SourceRunInput
from .diagnostics import diagnostic_from_exception


def normalize_compile_phase(phase: str) -> str:
    normalized = str(phase or "").strip().lower()
    if normalized not in {"config", "planning"}:
        raise InternalCompilerError("compile phase must be one of: config, planning")
    return normalized


def failed_run_from_source_input(
    *,
    source_input: SourceRunInput,
    total_runs: int,
    phase: str,
    exc: Exception,
    spec: Any | None = None,
) -> FailedCompiledRun:
    run_cfg = source_input.run_cfg
    project = spec.project if spec is not None else str(run_cfg.get("project", "") or "<unknown>")
    experiment_name = spec.experiment_name if spec is not None else str(run_cfg.get("experiment_name", "") or "<unknown>")
    model_name = spec.model.name if spec is not None and spec.model is not None else infer_model_name_from_cfg(run_cfg)
    train_mode = spec.run.mode if spec is not None else infer_train_mode_from_cfg(run_cfg)
    category = "config" if phase == "config" else "planning"
    code = "config_error" if phase == "config" else "planning_error"
    return FailedCompiledRun(
        run_index=source_input.source_index,
        total_runs=total_runs,
        project=project,
        experiment_name=experiment_name,
        model_name=model_name,
        train_mode=train_mode,
        phase=phase,
        source_label=source_input.source.config_label,
        sweep_case_name=source_input.sweep_case_name,
        sweep_assignments=dict(source_input.sweep_assignments),
        diagnostics=diagnostic_from_exception(exc, category=category, code=code, stage=phase),
    )


def planned_run_from_source_input(
    *,
    spec: Any,
    source_input: SourceRunInput,
    total_runs: int,
    identity: BatchIdentity,
) -> PlannedRun:
    source_ref = source_input.source
    return build_planned_run(
        spec,
        run_index=source_input.source_index,
        total_runs=total_runs,
        identity=identity,
        sweep_case_name=source_input.sweep_case_name,
        sweep_assignments=dict(source_input.sweep_assignments),
        replay_source_batch_root=(
            None if source_ref.source_batch_root is None else str(source_ref.source_batch_root)
        ),
        replay_source_run_id=source_ref.source_run_id,
        replay_source_record_path=(
            None if source_ref.source_record_path is None else str(source_ref.source_record_path)
        ),
    )
