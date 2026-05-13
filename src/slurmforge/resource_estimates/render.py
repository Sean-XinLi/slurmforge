from __future__ import annotations

from .models import ExperimentResourceEstimate


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
            lines.append(f"  {prefix}.mode: {sizing.mode}")
            if sizing.gpu_type:
                lines.append(f"  {prefix}.gpu_type: {sizing.gpu_type}")
            if sizing.estimator:
                lines.append(f"  {prefix}.estimator: {sizing.estimator}")
            if sizing.target_memory_gb is not None:
                lines.append(
                    f"  {prefix}.target_memory_gb: {sizing.target_memory_gb}"
                )
            if sizing.usable_memory_per_gpu_gb is not None:
                lines.append(
                    f"  {prefix}.usable_memory_per_gpu_gb: {sizing.usable_memory_per_gpu_gb}"
                )
            lines.append(
                f"  {prefix}.resolved_gpus_per_node: {sizing.resolved_gpus_per_node}"
            )
            lines.append(
                f"  {prefix}.total_gpus_per_run: {sizing.resolved_total_gpus}"
            )
        for group in stage.resource_groups:
            throttle = (
                "-" if group.array_throttle is None else str(group.array_throttle)
            )
            lines.append(
                f"  group {group.group_id}: runs={group.runs} "
                f"gpus_per_run={group.gpus_per_task} throttle={throttle} "
                f"peak_gpus={group.peak_concurrent_gpus}"
            )
        for warning in stage.warnings:
            lines.append(f"  warning: {warning}")
    return lines
