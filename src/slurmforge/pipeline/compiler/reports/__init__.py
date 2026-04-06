from __future__ import annotations

from .actions import raise_for_failures, require_success
from .builders import build_compile_failure_report, build_materialized_report, build_report
from .errors import BatchCompileError
from .models import BatchCompileReport, SourceCollectionReport
from .summary import (
    report_can_materialize,
    report_config_failure_count,
    report_diagnostics,
    report_has_failures,
    report_planned_run_count,
    report_planning_failure_count,
    report_source_failure_count,
    report_total_failed_runs,
    report_total_runs,
    report_warning_count,
)

__all__ = [
    "BatchCompileError",
    "BatchCompileReport",
    "SourceCollectionReport",
    "build_compile_failure_report",
    "build_materialized_report",
    "build_report",
    "report_can_materialize",
    "report_config_failure_count",
    "report_diagnostics",
    "report_has_failures",
    "report_planned_run_count",
    "report_planning_failure_count",
    "report_source_failure_count",
    "report_total_failed_runs",
    "report_total_runs",
    "report_warning_count",
    "raise_for_failures",
    "require_success",
]
