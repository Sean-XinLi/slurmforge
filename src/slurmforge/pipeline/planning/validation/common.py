from __future__ import annotations

from ..models import PlanDiagnostic, StageExecutionPlan


def build_diagnostic(
    *,
    severity: str,
    category: str,
    plan: StageExecutionPlan,
    code: str,
    message: str,
    field_path: str = "",
    actual: object = None,
    expected: object = None,
    hint: str = "",
) -> PlanDiagnostic:
    return PlanDiagnostic(
        severity=severity,
        category=category,
        code=code,
        message=message,
        stage=plan.name,
        field_path=field_path,
        actual=actual,
        expected=expected,
        hint=hint,
    )
