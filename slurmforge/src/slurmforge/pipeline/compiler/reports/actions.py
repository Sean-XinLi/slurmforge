from __future__ import annotations

from ...planning import PlannedBatch
from .errors import BatchCompileError
from .models import BatchCompileReport
from .summary import report_can_materialize, report_has_failures


def require_success(report: BatchCompileReport) -> PlannedBatch:
    if not report_can_materialize(report):
        raise BatchCompileError(report)
    return PlannedBatch(
        identity=report.identity,
        planned_runs=report.successful_runs,
        notify_cfg=report.notify_cfg,
        submit_dependencies=report.submit_dependencies,
        manifest_extras=report.manifest_extras,
    )


def raise_for_failures(report: BatchCompileReport) -> None:
    if report_has_failures(report):
        raise BatchCompileError(report)
