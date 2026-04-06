from __future__ import annotations

from dataclasses import replace

from ....errors import PlanningError
from ...config.runtime import ValidationConfig
from ..enums import DiagnosticCategory, DiagnosticSeverity
from ..models import PlanDiagnostic


def policy_value(raw: str | None, *, field_name: str) -> str:
    normalized = (raw or "").strip().lower() or "off"
    aliases = {"strict": "error"}
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"off", "warn", "error"}:
        raise PlanningError(f"{field_name} must be one of: off, warn, error")
    return normalized


def apply_validation_policies(
    diagnostics: list[PlanDiagnostic],
    *,
    policy: ValidationConfig,
) -> list[PlanDiagnostic]:
    topology_policy = policy_value(policy.topology_errors, field_name="validation.topology_errors")
    resource_policy = policy_value(policy.resource_warnings, field_name="validation.resource_warnings")
    normalized: list[PlanDiagnostic] = []
    for diagnostic in diagnostics:
        if (
            diagnostic.severity == DiagnosticSeverity.ERROR
            and diagnostic.category == DiagnosticCategory.TOPOLOGY
            and topology_policy == "off"
        ):
            normalized.append(replace(diagnostic, severity="warning"))
            continue
        if (
            diagnostic.severity == DiagnosticSeverity.WARNING
            and diagnostic.category == DiagnosticCategory.RESOURCE
            and resource_policy == "off"
        ):
            continue
        if (
            diagnostic.severity == DiagnosticSeverity.WARNING
            and diagnostic.category == DiagnosticCategory.RESOURCE
            and resource_policy == "error"
        ):
            normalized.append(replace(diagnostic, severity="error"))
            continue
        normalized.append(diagnostic)
    return normalized
