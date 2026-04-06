from __future__ import annotations

from ...codecs import serialize_execution_topology
from ...enums import LauncherKind
from ...models import PlanDiagnostic, StageExecutionPlan
from ..common import build_diagnostic


def build_topology_diagnostics(plan: StageExecutionPlan) -> list[PlanDiagnostic]:
    diagnostics: list[PlanDiagnostic] = []
    topology = plan.topology
    allocation = plan.allocation
    capabilities = plan.capabilities

    if plan.launcher_kind == LauncherKind.SINGLE and (topology.nodes != 1 or topology.processes_per_node != 1):
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="single_topology_invalid",
                message="single launcher requires topology nodes=1 and processes_per_node=1",
                field_path="topology",
                actual=serialize_execution_topology(topology),
                expected={"nodes": 1, "processes_per_node": 1},
            )
        )

    if plan.launcher_kind == LauncherKind.DDP and not capabilities.ddp_supported:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="ddp_unsupported",
                message="DDP launcher selected but stage capabilities do not support DDP",
                field_path="launcher_kind",
            )
        )

    if capabilities.ddp_required and topology.total_processes <= 1:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="ddp_required",
                message="stage requires distributed launch but topology resolves to a single process",
                field_path="topology",
                actual=serialize_execution_topology(topology),
                hint="increase topology or relax the stage capability contract",
            )
        )

    if topology.nodes > allocation.nodes:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="runtime_nodes_exceed_allocation",
                message=f"runtime nodes={topology.nodes} exceeds allocation nodes={allocation.nodes}",
                field_path="allocation.nodes",
                actual=topology.nodes,
                expected=allocation.nodes,
            )
        )

    if topology.processes_per_node > allocation.gpus_per_node:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="runtime_processes_exceed_gpus",
                message=(
                    f"runtime processes_per_node={topology.processes_per_node} exceeds "
                    f"allocation gpus_per_node={allocation.gpus_per_node}"
                ),
                field_path="allocation.gpus_per_node",
                actual=topology.processes_per_node,
                expected=allocation.gpus_per_node,
            )
        )

    if plan.max_available_gpus_per_node > 0 and allocation.gpus_per_node > plan.max_available_gpus_per_node:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="resource",
                plan=plan,
                code="allocation_exceeds_cluster_limit",
                message=(
                    f"allocation gpus_per_node={allocation.gpus_per_node} exceeds "
                    f"resources.max_available_gpus={plan.max_available_gpus_per_node}"
                ),
                field_path="allocation.gpus_per_node",
                actual=allocation.gpus_per_node,
                expected=plan.max_available_gpus_per_node,
            )
        )

    if plan.launcher_kind == LauncherKind.DDP and topology.total_processes > 1 and topology.master_port is None:
        diagnostics.append(
            build_diagnostic(
                severity="error",
                category="topology",
                plan=plan,
                code="ddp_missing_master_port",
                message="DDP topology requires a master_port",
                field_path="topology.master_port",
            )
        )

    return diagnostics
