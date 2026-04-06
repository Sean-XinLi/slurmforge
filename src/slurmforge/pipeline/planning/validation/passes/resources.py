from __future__ import annotations

from ...enums import InvocationKind, LauncherKind
from ...models import PlanDiagnostic, StageExecutionPlan
from ..common import build_diagnostic


def build_resource_diagnostics(plan: StageExecutionPlan) -> list[PlanDiagnostic]:
    diagnostics: list[PlanDiagnostic] = []
    topology = plan.topology
    allocation = plan.allocation
    estimate = plan.estimate

    if allocation.gpus_per_node > topology.processes_per_node:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="allocation_gpus_overprovisioned",
                message=(
                    f"allocation reserves {allocation.gpus_per_node} GPU(s)/node but runtime starts "
                    f"{topology.processes_per_node} process(es)/node"
                ),
                field_path="allocation.gpus_per_node",
                actual=allocation.gpus_per_node,
                expected=topology.processes_per_node,
                hint="reduce cluster.gpus_per_node or raise topology.processes_per_node",
            )
        )

    if allocation.nodes > topology.nodes:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="allocation_nodes_overprovisioned",
                message=f"allocation reserves {allocation.nodes} node(s) but runtime uses {topology.nodes}",
                field_path="allocation.nodes",
                actual=allocation.nodes,
                expected=topology.nodes,
                hint="reduce cluster.nodes or raise topology.nodes",
            )
        )

    if plan.launcher_kind == LauncherKind.SINGLE and allocation.total_gpus > 1:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="single_launcher_multigpu_allocation",
                message=f"single launcher reserves {allocation.total_gpus} GPU(s) but runs one process",
                field_path="launcher_kind",
                actual=allocation.total_gpus,
                expected=1,
                hint="switch to ddp or lower the Slurm allocation",
            )
        )

    if allocation.cpus_per_task > 0 and allocation.cpus_per_task < topology.processes_per_node:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="cpu_underprovisioned",
                message=(
                    f"allocation cpus_per_task={allocation.cpus_per_task} is lower than "
                    f"processes_per_node={topology.processes_per_node}"
                ),
                field_path="allocation.cpus_per_task",
                actual=allocation.cpus_per_task,
                expected=topology.processes_per_node,
                hint="increase cpus_per_task to avoid dataloader starvation",
            )
        )

    if estimate.recommended_total_gpus > allocation.total_gpus:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="allocation_below_recommendation",
                message=(
                    f"allocation total_gpus={allocation.total_gpus} is below the estimator recommendation "
                    f"total_gpus={estimate.recommended_total_gpus}"
                ),
                field_path="allocation",
                actual=allocation.total_gpus,
                expected=estimate.recommended_total_gpus,
                hint="expect OOM or reduce workload size",
            )
        )

    if allocation.total_gpus > estimate.max_useful_total_gpus:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="allocation_above_max_useful",
                message=(
                    f"allocation total_gpus={allocation.total_gpus} exceeds the estimator max useful "
                    f"total_gpus={estimate.max_useful_total_gpus}"
                ),
                field_path="allocation",
                actual=allocation.total_gpus,
                expected=estimate.max_useful_total_gpus,
                hint="the stage is likely over-provisioned",
            )
        )
    elif allocation.total_gpus > estimate.recommended_total_gpus:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="allocation_above_recommendation",
                message=(
                    f"allocation total_gpus={allocation.total_gpus} exceeds the estimator recommendation "
                    f"total_gpus={estimate.recommended_total_gpus}"
                ),
                field_path="allocation",
                actual=allocation.total_gpus,
                expected=estimate.recommended_total_gpus,
                hint="this is valid but may waste GPU capacity",
            )
        )

    if plan.invocation_kind == InvocationKind.EXTERNAL_COMMAND and topology.total_processes > estimate.max_useful_total_gpus:
        diagnostics.append(
            build_diagnostic(
                severity="warning",
                category="resource",
                plan=plan,
                code="external_runtime_above_estimate",
                message=(
                    f"external runtime total_processes={topology.total_processes} exceeds the estimator max useful "
                    f"total_gpus={estimate.max_useful_total_gpus}"
                ),
                field_path="topology",
                actual=topology.total_processes,
                expected=estimate.max_useful_total_gpus,
                hint="verify run.external_runtime against the actual external launcher contract",
            )
        )

    return diagnostics
