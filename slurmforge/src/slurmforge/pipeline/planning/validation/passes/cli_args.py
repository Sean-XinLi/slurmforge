from __future__ import annotations

from .....model_support.argparse_introspect import (
    extract_cli_arg_error,
    extract_supported_cli_keys,
    key_supported,
)
from ...models import PlanDiagnostic, StageExecutionPlan
from ..common import build_diagnostic


def build_cli_args_diagnostics(
    plan: StageExecutionPlan,
    *,
    cli_policy: str,
) -> list[PlanDiagnostic]:
    if plan.script_path is None or not plan.cli_args or cli_policy == "off":
        return []

    supported = extract_supported_cli_keys(str(plan.script_path))
    if not supported:
        failure = extract_cli_arg_error(str(plan.script_path))
        if failure is None:
            return []
        severity = "error" if cli_policy == "error" else "warning"
        return [
            build_diagnostic(
                severity=severity,
                category="cli_args",
                plan=plan,
                code="cli_args_introspection_failed",
                message=f"unable to introspect argparse args from {plan.script_path.name}: {failure}",
                field_path="cli_args",
                hint="declare args via argparse or relax validation.cli_args",
            )
        ]

    unknown = [key for key in plan.cli_args.keys() if not key_supported(key, supported)]
    if not unknown:
        return []
    severity = "error" if cli_policy == "error" else "warning"
    return [
        build_diagnostic(
            severity=severity,
            category="cli_args",
            plan=plan,
            code="cli_args_unknown",
            message=f"found args not declared via argparse in {plan.script_path.name}: {unknown}",
            field_path="cli_args",
            actual=unknown,
            hint="remove unsupported args or update the target script argparse schema",
        )
    ]
