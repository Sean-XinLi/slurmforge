from __future__ import annotations

from ...config.runtime import ValidationConfig
from ..enums import DiagnosticSeverity
from ..models import StageExecutionPlan
from .errors import PlanningValidationError
from .passes.cli_args import build_cli_args_diagnostics
from .passes.resources import build_resource_diagnostics
from .passes.summary import build_summary_diagnostics
from .passes.topology import build_topology_diagnostics
from .policies import apply_validation_policies, policy_value


def validate_stage_execution_plan(plan: StageExecutionPlan, policy: ValidationConfig) -> StageExecutionPlan:
    cli_policy = policy_value(policy.cli_args, field_name="validation.cli_args")
    diagnostics = (
        build_cli_args_diagnostics(plan, cli_policy=cli_policy)
        + build_topology_diagnostics(plan)
        + build_resource_diagnostics(plan)
        + build_summary_diagnostics(plan)
    )
    validated = plan.with_diagnostics(apply_validation_policies(diagnostics, policy=policy))
    if any(item.severity == DiagnosticSeverity.ERROR for item in validated.diagnostics):
        raise PlanningValidationError(validated)
    return validated
