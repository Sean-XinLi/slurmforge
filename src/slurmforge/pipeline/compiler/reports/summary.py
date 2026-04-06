from __future__ import annotations

from typing import Any

from ...planning.contracts import PlanDiagnostic
from ...planning.enums import DiagnosticSeverity
from .models import BatchCompileReport


def _attr(report: Any, name: str, default: Any) -> Any:
    return getattr(report, name, default)


def report_total_runs(report: BatchCompileReport | Any) -> int:
    checked_runs = int(_attr(report, "checked_runs", 0) or 0)
    if checked_runs:
        return checked_runs
    if hasattr(report, "successful_runs") or hasattr(report, "failed_runs"):
        return len(_attr(report, "successful_runs", ())) + len(_attr(report, "failed_runs", ()))
    return int(_attr(report, "total_runs", 0) or 0)


def report_planned_run_count(report: BatchCompileReport | Any) -> int:
    if hasattr(report, "successful_runs"):
        return len(_attr(report, "successful_runs", ()))
    return max(0, report_total_runs(report) - report_total_failed_runs(report))


def report_total_failed_runs(report: BatchCompileReport | Any) -> int:
    if hasattr(report, "failed_runs"):
        return len(_attr(report, "failed_runs", ()))
    return int(_attr(report, "failed_run_count", 0) or 0)


def report_source_failure_count(report: BatchCompileReport | Any) -> int:
    return sum(1 for item in _attr(report, "failed_runs", ()) if item.phase == "source")


def report_config_failure_count(report: BatchCompileReport | Any) -> int:
    return sum(1 for item in _attr(report, "failed_runs", ()) if item.phase == "config")


def report_planning_failure_count(report: BatchCompileReport | Any) -> int:
    return sum(1 for item in _attr(report, "failed_runs", ()) if item.phase == "planning")


def report_has_failures(report: BatchCompileReport | Any) -> bool:
    batch_diagnostics = _attr(report, "batch_diagnostics", ())
    if any(item.severity == DiagnosticSeverity.ERROR for item in batch_diagnostics):
        return True
    if hasattr(report, "failed_runs"):
        return bool(_attr(report, "failed_runs", ()))
    return bool(_attr(report, "has_failures", False))


def report_can_materialize(report: BatchCompileReport | Any) -> bool:
    return _attr(report, "identity", None) is not None and not report_has_failures(report)


def report_warning_count(report: BatchCompileReport | Any) -> int:
    if hasattr(report, "warning_count"):
        return int(_attr(report, "warning_count", 0) or 0)
    return sum(
        1
        for planned_run in _attr(report, "successful_runs", ())
        for diagnostic in planned_run.plan.planning_diagnostics
        if diagnostic.severity == DiagnosticSeverity.WARNING
    )


def report_diagnostics(report: BatchCompileReport | Any) -> tuple[PlanDiagnostic, ...]:
    batch_diagnostics = tuple(_attr(report, "batch_diagnostics", ()))
    return batch_diagnostics + tuple(
        diagnostic
        for failed_run in _attr(report, "failed_runs", ())
        for diagnostic in failed_run.diagnostics
    )
