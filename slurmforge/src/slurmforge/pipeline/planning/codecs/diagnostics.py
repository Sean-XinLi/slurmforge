from __future__ import annotations

from typing import Any

from ..models.diagnostics import PlanDiagnostic, coerce_plan_diagnostic


def parse_plan_diagnostic(value: Any, *, name: str = "diagnostic") -> PlanDiagnostic:
    return coerce_plan_diagnostic(value, name=name)


def serialize_plan_diagnostic(diagnostic: PlanDiagnostic) -> dict[str, Any]:
    return {
        "severity": diagnostic.severity.value,
        "category": diagnostic.category.value,
        "code": diagnostic.code,
        "message": diagnostic.message,
        "stage": diagnostic.stage,
        "field_path": diagnostic.field_path,
        "actual": diagnostic.actual,
        "expected": diagnostic.expected,
        "hint": diagnostic.hint,
    }
