from __future__ import annotations

from ...errors import InternalCompilerError
from ..planning.contracts import PlanDiagnostic
from ..planning.validator import PlanningValidationError


def diagnostic_from_exception(
    exc: Exception,
    *,
    category: str,
    code: str,
    stage: str,
) -> tuple[PlanDiagnostic, ...]:
    if isinstance(exc, PlanningValidationError):
        return tuple(exc.diagnostics)
    return (
        PlanDiagnostic(
            severity="error",
            category=category,
            code=code,
            message=str(exc).strip() or exc.__class__.__name__,
            stage=stage,
        ),
    )


def raise_internal_compiler_error(
    exc: Exception,
    *,
    context: str,
    run_index: int | None = None,
    total_runs: int | None = None,
    sweep_case_name: str | None = None,
) -> None:
    details = context
    if run_index is not None and total_runs is not None:
        details = f"{details} run {run_index}/{total_runs}"
    if sweep_case_name:
        details = f"{details} case={sweep_case_name}"
    raise InternalCompilerError(
        f"internal compiler error while {details}: {exc.__class__.__name__}: {exc}"
    ) from exc
