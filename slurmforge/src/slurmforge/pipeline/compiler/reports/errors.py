from __future__ import annotations

from ....errors import ConfigContractError
from ...planning.validator import format_diagnostic
from .models import BatchCompileReport
from .summary import report_diagnostics


class BatchCompileError(ConfigContractError):
    def __init__(self, report: BatchCompileReport):
        self.report = report
        self.batch_diagnostics = tuple(report.batch_diagnostics)
        self.failed_runs = tuple(report.failed_runs)
        self.diagnostics = tuple(report_diagnostics(report))
        if self.diagnostics:
            message = "\n".join(format_diagnostic(item) for item in self.diagnostics)
        else:
            message = "batch compilation failed"
        super().__init__(message)
