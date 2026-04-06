from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from ....errors import PlanningError
from ..enums import DiagnosticCategory, DiagnosticSeverity, coerce_enum


@dataclass(frozen=True)
class PlanDiagnostic:
    severity: DiagnosticSeverity
    category: DiagnosticCategory
    code: str
    message: str
    stage: str
    field_path: str = ""
    actual: Any = None
    expected: Any = None
    hint: str = ""

    def __post_init__(self) -> None:
        severity = coerce_enum(DiagnosticSeverity, self.severity, field_name="PlanDiagnostic.severity")
        category = coerce_enum(DiagnosticCategory, self.category, field_name="PlanDiagnostic.category")
        code = str(self.code or "").strip()
        message = str(self.message or "").strip()
        stage = str(self.stage or "").strip()
        if not code:
            raise PlanningError("PlanDiagnostic.code must be non-empty")
        if not message:
            raise PlanningError("PlanDiagnostic.message must be non-empty")
        if not stage:
            raise PlanningError("PlanDiagnostic.stage must be non-empty")
        object.__setattr__(self, "severity", severity)
        object.__setattr__(self, "category", category)
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "message", message)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "field_path", str(self.field_path or "").strip())
        object.__setattr__(self, "actual", copy.deepcopy(self.actual))
        object.__setattr__(self, "expected", copy.deepcopy(self.expected))
        object.__setattr__(self, "hint", str(self.hint or "").strip())


def coerce_plan_diagnostic(value: Any, *, name: str = "diagnostic") -> PlanDiagnostic:
    if isinstance(value, PlanDiagnostic):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return PlanDiagnostic(
        severity=value.get("severity", ""),
        category=value.get("category", ""),
        code=str(value.get("code", "") or ""),
        message=str(value.get("message", "") or ""),
        stage=str(value.get("stage", "") or ""),
        field_path=str(value.get("field_path", "") or ""),
        actual=copy.deepcopy(value.get("actual")),
        expected=copy.deepcopy(value.get("expected")),
        hint=str(value.get("hint", "") or ""),
    )
