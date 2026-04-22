from __future__ import annotations

from typing import Any, Sequence

from ....errors import ConfigContractError, PlanningError
from ...config.api import StorageConfigSpec
from ...config.normalize import normalize_dispatch
from ...config.runtime import DispatchConfig, NotifyConfig
from ...config.runtime.defaults import DEFAULT_RESOURCES
from ...planning import BatchIdentity, GpuBudgetPlan, PlannedRun, plan_gpu_budget
from ...planning.contracts import PlanDiagnostic
from ...sources.models import FailedCompiledRun
from ..batch_scope import resolve_batch_scope_unique
from ..state import MaterializedSourceBundle
from .models import BatchCompileReport
from .validator import validate_compile_report


def _batch_scope_error_diagnostic(exc: Exception, *, code: str, field_path: str) -> PlanDiagnostic:
    return PlanDiagnostic(
        severity="error",
        category="resource",
        code=code,
        message=str(exc),
        stage="batch",
        field_path=field_path,
    )


def _resolve_batch_scope(
    *,
    max_available_gpus_candidates: Sequence[int],
    dispatch_policy_candidates: Sequence[str],
) -> tuple[int | None, DispatchConfig | None, tuple[PlanDiagnostic, ...]]:
    """Collapse CompileState candidates into batch-scoped values.

    Returns ``(max_available_gpus, dispatch_cfg, diagnostics)``.  If
    candidates disagree and no consensus is possible, the diagnostics
    tuple carries the errors and the scalar / config entries are ``None``.

    When there are no candidates (batch has no successful runs), falls
    back to the package defaults so the report still has a well-formed
    batch-scoped snapshot.
    """
    diagnostics: list[PlanDiagnostic] = []
    max_available_gpus: int | None
    if max_available_gpus_candidates:
        try:
            max_available_gpus = resolve_batch_scope_unique(
                tuple(max_available_gpus_candidates),
                field_path="resources.max_available_gpus",
            )
        except ConfigContractError as exc:
            diagnostics.append(
                _batch_scope_error_diagnostic(
                    exc,
                    code="batch_scope_inconsistent_max_available_gpus",
                    field_path="resources.max_available_gpus",
                )
            )
            max_available_gpus = None
    else:
        max_available_gpus = int(DEFAULT_RESOURCES["max_available_gpus"])

    dispatch_cfg: DispatchConfig | None
    if dispatch_policy_candidates:
        try:
            resolved_policy = resolve_batch_scope_unique(
                tuple(dispatch_policy_candidates),
                field_path="dispatch.group_overflow_policy",
            )
            dispatch_cfg = normalize_dispatch({"group_overflow_policy": resolved_policy})
        except ConfigContractError as exc:
            diagnostics.append(
                _batch_scope_error_diagnostic(
                    exc,
                    code="batch_scope_inconsistent_dispatch_policy",
                    field_path="dispatch.group_overflow_policy",
                )
            )
            dispatch_cfg = None
    else:
        dispatch_cfg = DispatchConfig()

    return max_available_gpus, dispatch_cfg, tuple(diagnostics)


def _compute_budget_plan(
    successful_runs: Sequence[PlannedRun],
    *,
    max_available_gpus: int,
    dispatch_cfg: DispatchConfig,
) -> tuple[GpuBudgetPlan | None, tuple[PlanDiagnostic, ...]]:
    if not successful_runs:
        return None, ()
    try:
        plan = plan_gpu_budget(
            successful_runs,
            max_available_gpus=max_available_gpus,
            dispatch_cfg=dispatch_cfg,
        )
    except (ConfigContractError, PlanningError) as exc:
        return None, (_batch_scope_error_diagnostic(
            exc,
            code="gpu_budget_overflow",
            field_path="resources.max_available_gpus",
        ),)
    return plan, ()


def build_report(
    *,
    identity: BatchIdentity | None,
    successful_runs: Sequence[PlannedRun],
    failed_runs: Sequence[FailedCompiledRun],
    batch_diagnostics: Sequence[PlanDiagnostic],
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    manifest_extras: dict[str, Any],
    source_summary: str,
    storage_config: StorageConfigSpec | None = None,
    max_available_gpus: int | None = None,
    dispatch_cfg: DispatchConfig | None = None,
    gpu_budget_plan: GpuBudgetPlan | None = None,
) -> BatchCompileReport:
    return validate_compile_report(
        BatchCompileReport(
            identity=identity,
            successful_runs=tuple(successful_runs),
            failed_runs=tuple(failed_runs),
            batch_diagnostics=tuple(batch_diagnostics),
            checked_runs=checked_runs,
            notify_cfg=notify_cfg,
            submit_dependencies={} if submit_dependencies is None else submit_dependencies,
            manifest_extras=manifest_extras,
            source_summary=source_summary,
            storage_config=storage_config if storage_config is not None else StorageConfigSpec(),
            max_available_gpus=(
                max_available_gpus
                if max_available_gpus is not None
                else int(DEFAULT_RESOURCES["max_available_gpus"])
            ),
            dispatch_cfg=dispatch_cfg if dispatch_cfg is not None else DispatchConfig(),
            gpu_budget_plan=gpu_budget_plan,
        )
    )


def build_materialized_report(
    *,
    materialized: MaterializedSourceBundle,
    identity: BatchIdentity | None,
    successful_runs: Sequence[PlannedRun],
    failed_runs: Sequence[FailedCompiledRun],
    batch_diagnostics: Sequence[PlanDiagnostic] | None = None,
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    storage_config: StorageConfigSpec | None = None,
    max_available_gpus_candidates: Sequence[int] = (),
    dispatch_policy_candidates: Sequence[str] = (),
) -> BatchCompileReport:
    diagnostics_from_bundle = (
        tuple(materialized.batch_diagnostics) if batch_diagnostics is None else tuple(batch_diagnostics)
    )

    # Resolve batch-scoped fields from the candidates the compile flow
    # accumulated.  Inconsistency becomes a batch-level error diagnostic
    # (same channel as other config errors), which means the report has
    # has_failures=True and require_success will refuse to proceed.
    resolved_max_available_gpus, resolved_dispatch, resolve_diagnostics = _resolve_batch_scope(
        max_available_gpus_candidates=max_available_gpus_candidates,
        dispatch_policy_candidates=dispatch_policy_candidates,
    )
    if resolve_diagnostics:
        diagnostics_from_bundle = diagnostics_from_bundle + resolve_diagnostics
        # Inconsistent batch-scoped values ⇒ cannot meaningfully plan the
        # budget, so mark all successful runs as pending until the user
        # resolves it.  Same pattern as budget overflow with policy=error.
        successful_runs = ()

    budget_plan: GpuBudgetPlan | None = None
    if resolved_max_available_gpus is not None and resolved_dispatch is not None:
        budget_plan, budget_diagnostics = _compute_budget_plan(
            successful_runs,
            max_available_gpus=resolved_max_available_gpus,
            dispatch_cfg=resolved_dispatch,
        )
        if budget_diagnostics:
            diagnostics_from_bundle = diagnostics_from_bundle + budget_diagnostics
            successful_runs = ()
        if budget_plan is not None and budget_plan.warnings:
            # Budget warnings flow through the same diagnostic channel as
            # everything else so validate / dry-run / any future consumer
            # surfaces them without a dedicated code path.
            diagnostics_from_bundle = diagnostics_from_bundle + tuple(budget_plan.warnings)

    return build_report(
        identity=identity,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        batch_diagnostics=diagnostics_from_bundle,
        checked_runs=checked_runs,
        notify_cfg=notify_cfg,
        submit_dependencies=submit_dependencies,
        manifest_extras=materialized.manifest_extras,
        source_summary=materialized.report.source_summary,
        storage_config=storage_config,
        max_available_gpus=resolved_max_available_gpus,
        dispatch_cfg=resolved_dispatch,
        gpu_budget_plan=budget_plan,
    )


def build_compile_failure_report(
    *,
    materialized: MaterializedSourceBundle,
    failed_runs: Sequence[FailedCompiledRun],
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    exc: Exception,
    category: str,
    code: str,
    stage: str = "batch",
    diagnostics_from_exception,
) -> BatchCompileReport:
    return build_materialized_report(
        materialized=materialized,
        identity=None,
        successful_runs=(),
        failed_runs=failed_runs,
        batch_diagnostics=diagnostics_from_exception(exc, category=category, code=code, stage=stage),
        checked_runs=checked_runs,
        notify_cfg=notify_cfg,
        submit_dependencies=submit_dependencies,
    )
