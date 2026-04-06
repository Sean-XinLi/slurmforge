from __future__ import annotations

import copy
from typing import Any

from ..planning.contracts import PlanDiagnostic
from .inference import infer_model_name_from_cfg, infer_train_mode_from_cfg
from .models import FailedCompiledRun


def source_diagnostic(message: str, *, code: str = "source_error") -> PlanDiagnostic:
    return PlanDiagnostic(
        severity="error",
        category="config",
        code=code,
        message=message,
        stage="source",
    )


def build_source_failure(
    *,
    source_index: int,
    total_inputs: int,
    source_label: str,
    run_cfg: dict[str, Any] | None,
    sweep_case_name: str | None,
    sweep_assignments: dict[str, Any],
    exc: Exception,
    code: str = "source_error",
) -> FailedCompiledRun:
    project = "<unknown>"
    experiment_name = "<unknown>"
    if isinstance(run_cfg, dict):
        project = str(run_cfg.get("project", "") or "<unknown>")
        experiment_name = str(run_cfg.get("experiment_name", "") or "<unknown>")
    return FailedCompiledRun(
        run_index=source_index,
        total_runs=total_inputs,
        project=project,
        experiment_name=experiment_name,
        model_name=infer_model_name_from_cfg(run_cfg),
        train_mode=infer_train_mode_from_cfg(run_cfg),
        phase="source",
        source_label=source_label,
        sweep_case_name=sweep_case_name,
        sweep_assignments=copy.deepcopy(sweep_assignments),
        diagnostics=(source_diagnostic(f"{source_label}: {exc}", code=code),),
    )
