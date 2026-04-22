from __future__ import annotations

import json
from typing import Any, Iterable

from ..config.runtime import ClusterConfig
from ..planning import GpuBudgetPlan, PlannedBatch
from ..records.models.run_plan import RunPlan
from .grouping import array_group_signature, array_grouping_fields, describe_array_group_reason


def _print_gpu_budget(plan: GpuBudgetPlan) -> None:
    print("[dry-run] GPU batch budget:")
    print(f"  max_available_gpus={plan.max_available_gpus}")
    print(f"  group_overflow_policy={plan.group_overflow_policy}")
    print(f"  policy_applied={plan.policy_applied}")
    print(f"  min_concurrent_gpus={plan.min_concurrent_gpus}")
    print(f"  max_planned_concurrent_gpus={plan.max_planned_concurrent_gpus}")
    print(f"  strict_global_limit={str(plan.strict_global_limit).lower()}")
    for group in plan.groups:
        print(f"  group {group.group_id}:")
        print(f"    tasks={group.task_count}")
        print(f"    gpus_per_task={group.gpus_per_task}")
        print(f"    throttle={group.throttle}")
        print(f"    max_group_gpus={group.max_group_gpus}")
        print(f"    max_estimated_gpus={group.max_estimated_gpus}")
        print(f"    limiting_run={group.limiting_run}")
        print(f"    limiting_model={group.limiting_model}")
        if group.constrained:
            print(f"    constrained=true reason={group.constraint_reason}")
    for warning in plan.warnings:
        print(f"  [{warning.severity.upper()}] {warning.code}: {warning.message}")


def print_dry_run(
    run_plans: Iterable[RunPlan],
    *,
    gpu_budget_plan: GpuBudgetPlan | None = None,
) -> None:
    group_counts: dict[str, tuple[int, ClusterConfig, dict[str, Any], str]] = {}
    for plan in run_plans:
        stage = plan.train_stage
        allocation = stage.allocation
        diagnostics = [item for item in plan.planning_diagnostics if item.severity in {"warning", "error"}]
        cluster_cfg = plan.cluster
        print(
            f"[dry-run] #{plan.run_index}/{plan.total_runs} model={plan.model_name} "
            f"launcher={stage.launcher_kind} estimated_gpus={stage.estimate.recommended_total_gpus} "
            f"requested_gpus={allocation.gpus_per_node}\n"
            f"  topology : nodes={stage.topology.nodes} processes_per_node={stage.topology.processes_per_node}\n"
            f"  resources: nodes={allocation.nodes} "
            f"gpus_per_node={allocation.gpus_per_node} "
            f"cpus_per_task={allocation.cpus_per_task} mem={allocation.mem}\n"
            f"  estimate : min={stage.estimate.min_total_gpus} "
            f"recommended={stage.estimate.recommended_total_gpus} max_useful={stage.estimate.max_useful_total_gpus}\n"
            f"  estimate_reason: {stage.estimate.reason}\n"
            f"  train: {stage.command_text}"
        )
        if plan.eval_stage is not None:
            print(f"  eval : {plan.eval_stage.command_text}")
        if diagnostics:
            for diagnostic in diagnostics:
                print(f"  {diagnostic.severity}: {diagnostic.code} {diagnostic.message}")
        signature = array_group_signature(cluster_cfg, plan.env)
        prev = group_counts.get(signature)
        if prev is None:
            group_counts[signature] = (
                1,
                cluster_cfg,
                array_grouping_fields(cluster_cfg, plan.env),
                describe_array_group_reason(),
            )
        else:
            group_counts[signature] = (prev[0] + 1, prev[1], prev[2], prev[3])

    print(f"[dry-run] dispatch_mode=array, array_groups={len(group_counts)}")
    for group_idx, (_signature, (count, cluster, grouping_fields, group_reason)) in enumerate(group_counts.items(), start=1):
        print(
            f"  - group {group_idx}: tasks={count} "
            f"partition={cluster.partition} gpus_per_node={cluster.gpus_per_node}\n"
            f"    reason={group_reason}\n"
            f"    fields={json.dumps(grouping_fields, sort_keys=True)}"
        )

    if gpu_budget_plan is not None:
        _print_gpu_budget(gpu_budget_plan)


def print_dry_run_batch(planned_batch: PlannedBatch) -> None:
    """Convenience wrapper that feeds the batch-wide GpuBudgetPlan alongside runs."""
    print_dry_run(
        (planned_run.plan for planned_run in planned_batch.planned_runs),
        gpu_budget_plan=planned_batch.gpu_budget_plan,
    )
