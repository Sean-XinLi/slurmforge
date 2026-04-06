from __future__ import annotations

from ...models import PlanDiagnostic, StageExecutionPlan
from ..common import build_diagnostic


def build_summary_diagnostics(plan: StageExecutionPlan) -> list[PlanDiagnostic]:
    topology = plan.topology
    allocation = plan.allocation
    return [
        build_diagnostic(
            severity="info",
            category="summary",
            plan=plan,
            code="plan_summary",
            message=(
                f"resolved launcher={plan.launcher_kind} topology={topology.nodes}x{topology.processes_per_node} "
                f"allocation={allocation.nodes}x{allocation.gpus_per_node}"
            ),
        )
    ]
