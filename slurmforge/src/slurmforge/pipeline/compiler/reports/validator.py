from __future__ import annotations

from ....errors import InternalCompilerError
from ...planning import validate_planned_batch_runs
from .models import BatchCompileReport


def validate_compile_report(report: BatchCompileReport) -> BatchCompileReport:
    checked_runs = int(report.checked_runs or 0)
    if checked_runs and len(report.successful_runs) + len(report.failed_runs) > checked_runs:
        raise InternalCompilerError("BatchCompileReport counts exceed checked_runs")
    if report.identity is not None and report.successful_runs and not report.failed_runs and not report.batch_diagnostics:
        validate_planned_batch_runs(report.successful_runs, identity=report.identity)
    return report
