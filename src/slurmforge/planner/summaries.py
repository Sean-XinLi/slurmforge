from __future__ import annotations

from typing import Any

from ..io import to_jsonable
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan


def summarize_stage_batch(batch: StageBatchPlan) -> list[str]:
    lines = [
        f"[PLAN] stage_batch={batch.batch_id} stage={batch.stage_name} runs={len(batch.selected_runs)} root={batch.submission_root}",
    ]
    for group in batch.group_plans:
        throttle = "-" if group.array_throttle is None else str(group.array_throttle)
        lines.append(
            f"[PLAN] {group.group_id} runs={group.array_size} gpus_per_task={group.gpus_per_task} throttle={throttle}"
        )
    for dep in batch.budget_plan.dependencies:
        from_text = ",".join(str(item) for item in dep.from_groups if item)
        lines.append(
            f"[PLAN] dependency {dep.to_group} after {from_text} type={dep.type}"
        )
    for warning in batch.budget_plan.warnings:
        lines.append(f"[WARN] {warning}")
    return lines


def summarize_train_eval_pipeline_plan(plan: TrainEvalPipelinePlan) -> list[str]:
    lines = [
        f"[PLAN] train_eval_pipeline={plan.pipeline_id} stages={' -> '.join(plan.stage_order)} runs={len(plan.run_set)} root={plan.root_dir}",
    ]
    for stage_name in plan.stage_order:
        lines.extend(summarize_stage_batch(plan.stage_batches[stage_name]))
    return lines


def serialize_plan(plan: StageBatchPlan | TrainEvalPipelinePlan) -> dict[str, Any]:
    return to_jsonable(plan)
