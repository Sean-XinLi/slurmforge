from __future__ import annotations

from .engine import compile_materialized_bundle, resolve_strategy
from .flows import AUTHORING_FLOW, REPLAY_FLOW
from .planning_pass import normalize_compile_phase
from .reports import (
    BatchCompileError,
    BatchCompileReport,
    SourceCollectionReport,
    report_total_runs,
)
from .requests import (
    AuthoringSourceRequest,
    ReplaySourceRequest,
    RetrySourceRequest,
    SourceRequest,
)
from .state import CollectedSourceBundle, MaterializedSourceBundle
from ..planning.enums import DiagnosticSeverity
from ..planning.validator import format_diagnostic

STRATEGIES = (AUTHORING_FLOW, REPLAY_FLOW)


def _collect_source_bundle(source: SourceRequest) -> CollectedSourceBundle:
    strategy = resolve_strategy(source, strategies=STRATEGIES)
    return strategy.collect(source)


def _materialize_source_bundle(bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
    request = bundle.report.request
    strategy = resolve_strategy(request, strategies=STRATEGIES)
    return strategy.materialize(bundle)


def collect_source(source: SourceRequest) -> SourceCollectionReport:
    return _collect_source_bundle(source).report


def compile_source(
    source: SourceRequest,
    *,
    phase: str = "planning",
) -> BatchCompileReport:
    normalized_phase = normalize_compile_phase(phase)
    collected = _collect_source_bundle(source)
    materialized = _materialize_source_bundle(collected)
    strategy = resolve_strategy(materialized.report.request, strategies=STRATEGIES)
    return compile_materialized_bundle(
        materialized,
        include_planning=normalized_phase == "planning",
        strategy=strategy,
    )


def iter_compile_report_lines(report: BatchCompileReport) -> tuple[str, ...]:
    lines: list[str] = []
    for diagnostic in report.batch_diagnostics:
        lines.append(format_diagnostic(diagnostic))
    for failed_run in report.failed_runs:
        if failed_run.phase == "source":
            header = f"[ERROR] run {failed_run.run_index}/{failed_run.total_runs} phase=source"
        else:
            header = (
                f"[ERROR] run {failed_run.run_index}/{failed_run.total_runs} "
                f"phase={failed_run.phase} model={failed_run.model_name} train_mode={failed_run.train_mode}"
            )
        if failed_run.source_label:
            header = f"{header} source={failed_run.source_label}"
        if failed_run.sweep_case_name:
            header = f"{header} case={failed_run.sweep_case_name}"
        lines.append(header)
        for diagnostic in failed_run.diagnostics:
            lines.append(format_diagnostic(diagnostic))
    for planned_run in report.successful_runs:
        warnings = [item for item in planned_run.plan.planning_diagnostics if item.severity == DiagnosticSeverity.WARNING]
        if not warnings:
            continue
        lines.append(f"[WARN] run {planned_run.plan.run_index}/{report_total_runs(report)} {planned_run.plan.run_id}")
        for diagnostic in warnings:
            lines.append(format_diagnostic(diagnostic))
    return tuple(lines)


__all__ = [
    "AuthoringSourceRequest",
    "BatchCompileError",
    "BatchCompileReport",
    "ReplaySourceRequest",
    "RetrySourceRequest",
    "SourceCollectionReport",
    "SourceRequest",
    "collect_source",
    "compile_source",
    "iter_compile_report_lines",
]
