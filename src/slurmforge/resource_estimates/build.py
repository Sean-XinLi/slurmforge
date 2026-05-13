from __future__ import annotations

from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..sizing.models import GpuSizingResolution
from .models import (
    ExperimentResourceEstimate,
    ResourceGroupEstimate,
    StageResourceEstimate,
)


def build_stage_batch_resource_estimate(batch: StageBatchPlan) -> StageResourceEstimate:
    groups = batch.group_plans
    budget_plan = batch.budget_plan
    total_requested = sum(
        int(group.gpus_per_task) * int(group.array_size) for group in groups
    )
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
    seen: set[GpuSizingResolution] = set()
    run_sizing: list[GpuSizingResolution] = []
    for instance in batch.stage_instances:
        sizing = instance.resource_sizing
        if sizing in seen:
            continue
        seen.add(sizing)
        run_sizing.append(sizing)
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


def build_train_eval_pipeline_resource_estimate(
    plan: TrainEvalPipelinePlan,
) -> ExperimentResourceEstimate:
    batches = [plan.stage_batches[name] for name in plan.stage_order]
    stages = tuple(build_stage_batch_resource_estimate(batch) for batch in batches)
    first = batches[0]
    return ExperimentResourceEstimate(
        project=first.project,
        experiment=first.experiment,
        runs=len(plan.run_set),
        max_available_gpus=max(
            (stage.max_available_gpus for stage in stages), default=0
        ),
        stages=stages,
    )


def build_resource_estimate(
    plan: StageBatchPlan | TrainEvalPipelinePlan,
) -> ExperimentResourceEstimate:
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
