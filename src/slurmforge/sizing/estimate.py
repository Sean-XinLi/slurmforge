from __future__ import annotations

from typing import Any

from ..io import to_jsonable
from .models import ExperimentResourceEstimate, ResourceGroupEstimate, StageResourceEstimate


def _stage_batch_estimate(batch: Any) -> StageResourceEstimate:
    groups = tuple(getattr(batch, "group_plans", ()))
    budget_plan = getattr(batch, "budget_plan", None)
    total_requested = sum(int(group.gpus_per_task) * int(group.array_size) for group in groups)
    waves = tuple(getattr(budget_plan, "waves", ()) or ())
    if waves:
        peak = max(int(getattr(wave, "total_wave_gpus", 0) or 0) for wave in waves)
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
    for instance in getattr(batch, "stage_instances", ()):
        payload = to_jsonable(getattr(instance, "resource_sizing", {}) or {})
        key = repr(sorted(payload.items()))
        if key in seen:
            continue
        seen.add(key)
        run_sizing.append(payload)
    return StageResourceEstimate(
        stage_name=str(batch.stage_name),
        runs=len(tuple(getattr(batch, "selected_runs", ()))),
        max_available_gpus=int(getattr(budget_plan, "max_available_gpus", 0) or 0),
        total_requested_gpus=total_requested,
        peak_concurrent_gpus=peak,
        waves=wave_count,
        resource_groups=tuple(group_estimates),
        run_sizing=tuple(run_sizing),
        warnings=tuple(str(item) for item in getattr(budget_plan, "warnings", ()) or ()),
    )


def build_resource_estimate(plan: Any) -> ExperimentResourceEstimate:
    if hasattr(plan, "stage_batches"):
        batches = [plan.stage_batches[name] for name in plan.stage_order]
        stages = tuple(_stage_batch_estimate(batch) for batch in batches)
        first = batches[0]
        return ExperimentResourceEstimate(
            project=str(first.project),
            experiment=str(first.experiment),
            runs=len(tuple(plan.run_set)),
            max_available_gpus=max((stage.max_available_gpus for stage in stages), default=0),
            stages=stages,
        )
    stage = _stage_batch_estimate(plan)
    return ExperimentResourceEstimate(
        project=str(plan.project),
        experiment=str(plan.experiment),
        runs=len(tuple(plan.selected_runs)),
        max_available_gpus=stage.max_available_gpus,
        stages=(stage,),
    )


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
