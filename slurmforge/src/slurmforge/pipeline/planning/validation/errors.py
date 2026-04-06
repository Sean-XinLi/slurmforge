from __future__ import annotations

from ....errors import PlanningError
from ..enums import DiagnosticSeverity
from ..models import StageExecutionPlan
from .formatter import format_diagnostic


class PlanningValidationError(PlanningError):
    def __init__(self, plan: StageExecutionPlan):
        self.plan = plan
        self.diagnostics = tuple(plan.diagnostics)
        error_messages = [format_diagnostic(item) for item in self.diagnostics if item.severity == DiagnosticSeverity.ERROR]
        message = "\n".join(error_messages) if error_messages else f"{plan.name}: planning validation failed"
        super().__init__(message)
