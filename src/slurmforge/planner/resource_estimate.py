from __future__ import annotations

from ..io import to_jsonable
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..sizing import ExperimentResourceEstimate, ResourceGroupEstimate, StageResourceEstimate


def build_stage_batch_resource_estimate(batch: StageBatchPlan) -> StageResourceEstimate:
    groups = batch.group_plans
    budget_plan = batch.budget_plan
    total_requested = sum(int(group.gpus_per_task) * int(group.array_size) for group in groups)
    waves = budget_plan.waves
    if waves:
        peak = max(int(wave.total_wave_gpus or 0) for wave in waves)
        wave_count = len(waves)
    else:
        peak = total_requested
        wave_count = 1 if total_requested > 0 else 0
    group_estimates = []
    for group in groups:
        throttle = group.array_throttle
        active = int(group.array_size) if throttle is None else int(throttle)
        group_estimates.append(
            ResourceGroupEstimate(
                group_id=str(group.group_id),
                runs=int(group.array_size),
                gpus_per_task=int(group.gpus_per_task),
                array_throttle=throttle,
                peak_concurrent_gpus=int(group.gpus_per_task) * active,
            )
        )
    seen: set[str] = set()
    run_sizing = []
    for instance in batch.stage_instances:
        payload = to_jsonable(instance.resource_sizing)
        key = repr(sorted(payload.items()))
        if key in seen:
            continue
        seen.add(key)
        run_sizing.append(payload)
    return StageResourceEstimate(
        stage_name=batch.stage_name,
        runs=len(batch.selected_runs),
        max_available_gpus=budget_plan.max_available_gpus,
        total_requested_gpus=total_requested,
        peak_concurrent_gpus=peak,
        waves=wave_count,
        resource_groups=tuple(group_estimates),
        run_sizing=tuple(run_sizing),
        warnings=budget_plan.warnings,
    )


def build_train_eval_pipeline_resource_estimate(plan: TrainEvalPipelinePlan) -> ExperimentResourceEstimate:
    batches = [plan.stage_batches[name] for name in plan.stage_order]
    stages = tuple(build_stage_batch_resource_estimate(batch) for batch in batches)
    first = batches[0]
    return ExperimentResourceEstimate(
        project=first.project,
        experiment=first.experiment,
        runs=len(plan.run_set),
        max_available_gpus=max((stage.max_available_gpus for stage in stages), default=0),
        stages=stages,
    )


def build_resource_estimate(plan: StageBatchPlan | TrainEvalPipelinePlan) -> ExperimentResourceEstimate:
    if isinstance(plan, StageBatchPlan):
        stage = build_stage_batch_resource_estimate(plan)
        return ExperimentResourceEstimate(
            project=plan.project,
            experiment=plan.experiment,
            runs=len(plan.selected_runs),
            max_available_gpus=stage.max_available_gpus,
            stages=(stage,),
        )
    return build_train_eval_pipeline_resource_estimate(plan)


def render_resource_estimate(estimate: ExperimentResourceEstimate) -> list[str]:
    lines = [
        f"[ESTIMATE] project={estimate.project} experiment={estimate.experiment} runs={estimate.runs}",
        f"[ESTIMATE] max_available_gpus={estimate.max_available_gpus}",
    ]
    for stage in estimate.stages:
        lines.extend(
            [
                "",
                f"Stage {stage.stage_name}:",
                f"  runs: {stage.runs}",
                f"  total_requested_gpus: {stage.total_requested_gpus}",
                f"  peak_concurrent_gpus: {stage.peak_concurrent_gpus}",
                f"  waves: {stage.waves}",
            ]
        )
        for index, sizing in enumerate(stage.run_sizing, start=1):
            prefix = "sizing" if len(stage.run_sizing) == 1 else f"sizing[{index}]"
            lines.append(f"  {prefix}.mode: {sizing.get('mode', 'fixed')}")
            if sizing.get("gpu_type"):
                lines.append(f"  {prefix}.gpu_type: {sizing['gpu_type']}")
            if sizing.get("estimator"):
                lines.append(f"  {prefix}.estimator: {sizing['estimator']}")
            if sizing.get("target_memory_gb") is not None:
                lines.append(f"  {prefix}.target_memory_gb: {sizing['target_memory_gb']}")
            if sizing.get("usable_memory_per_gpu_gb") is not None:
                lines.append(f"  {prefix}.usable_memory_per_gpu_gb: {sizing['usable_memory_per_gpu_gb']}")
            lines.append(f"  {prefix}.resolved_gpus_per_node: {sizing.get('resolved_gpus_per_node', 0)}")
            lines.append(f"  {prefix}.total_gpus_per_run: {sizing.get('resolved_total_gpus', 0)}")
        for group in stage.resource_groups:
            throttle = "-" if group.array_throttle is None else str(group.array_throttle)
            lines.append(
                f"  group {group.group_id}: runs={group.runs} "
                f"gpus_per_run={group.gpus_per_task} throttle={throttle} "
                f"peak_gpus={group.peak_concurrent_gpus}"
            )
        for warning in stage.warnings:
            lines.append(f"  warning: {warning}")
    return lines
