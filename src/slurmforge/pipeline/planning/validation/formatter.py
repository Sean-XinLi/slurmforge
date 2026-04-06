from __future__ import annotations

from ..models import PlanDiagnostic


def format_diagnostic(diagnostic: PlanDiagnostic) -> str:
    field = f" [{diagnostic.field_path}]" if diagnostic.field_path else ""
    hint = f" hint={diagnostic.hint}" if diagnostic.hint else ""
    return f"{diagnostic.severity.upper()} {diagnostic.stage}:{diagnostic.code}{field} {diagnostic.message}{hint}"
