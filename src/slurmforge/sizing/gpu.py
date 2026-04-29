from __future__ import annotations

from math import ceil

from ..config_schema import options_for, options_sentence
from ..errors import ConfigContractError
from .models import (
    GpuHardwareProfile,
    GpuResourceSizingRequest,
    GpuSizingDefaults,
    GpuSizingResolution,
)
from .models import StageGpuSizingPolicy


def _is_auto(value: int | str) -> bool:
    return str(value).lower() == "auto"


def _ceil_to(value: int, quantum: int) -> int:
    if quantum <= 1:
        return int(value)
    return int(ceil(value / quantum) * quantum)


def resolve_stage_gpu_sizing(
    *,
    request: GpuResourceSizingRequest,
    gpu_sizing: StageGpuSizingPolicy | None,
    gpu_types: dict[str, GpuHardwareProfile],
    defaults: GpuSizingDefaults,
) -> GpuSizingResolution:
    stage_name = request.stage_name
    nodes = request.nodes
    raw_gpus_per_node = request.gpus_per_node
    gpu_type = request.gpu_type
    if not _is_auto(raw_gpus_per_node):
        if gpu_type and gpu_type not in gpu_types:
            raise ConfigContractError(
                f"`stages.{stage_name}.resources.gpu_type` references unknown gpu type `{gpu_type}`"
            )
        if gpu_sizing is not None:
            raise ConfigContractError(
                f"`stages.{stage_name}.gpu_sizing` is only allowed when gpus_per_node is auto"
            )
        resolved_gpus_per_node = int(raw_gpus_per_node or 0)
        return GpuSizingResolution(
            mode="fixed",
            stage_name=stage_name,
            nodes=nodes,
            gpu_type=gpu_type,
            resolved_gpus_per_node=resolved_gpus_per_node,
            resolved_total_gpus=nodes * resolved_gpus_per_node,
        )

    if not gpu_type:
        raise ConfigContractError(
            f"`stages.{stage_name}.resources.gpu_type` is required when gpus_per_node is auto"
        )
    if gpu_type not in gpu_types:
        raise ConfigContractError(
            f"`stages.{stage_name}.resources.gpu_type` references unknown gpu type `{gpu_type}`"
        )
    if gpu_sizing is None:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing` is required when gpus_per_node is auto"
        )

    estimator = gpu_sizing.estimator
    if estimator not in options_for("stages.*.gpu_sizing.estimator"):
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.estimator` must be "
            f"{options_sentence('stages.*.gpu_sizing.estimator')}"
        )
    target_memory_gb = gpu_sizing.target_memory_gb
    if target_memory_gb <= 0:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.target_memory_gb` must be > 0"
        )
    gpu_type_spec = gpu_types[gpu_type]
    memory_gb = gpu_type_spec.memory_gb
    usable_memory_fraction = gpu_type_spec.usable_memory_fraction
    max_gpus_per_node = gpu_type_spec.max_gpus_per_node
    if memory_gb <= 0:
        raise ConfigContractError(
            f"`hardware.gpu_types.{gpu_type}.memory_gb` must be > 0"
        )
    if usable_memory_fraction <= 0 or usable_memory_fraction > 1:
        raise ConfigContractError(
            f"`hardware.gpu_types.{gpu_type}.usable_memory_fraction` must be in (0, 1]"
        )

    safety_factor = gpu_sizing.safety_factor
    if safety_factor is None:
        safety_factor = defaults.safety_factor
    safety_factor = float(safety_factor)
    if safety_factor < 1:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.safety_factor` must be >= 1"
        )
    round_to = gpu_sizing.round_to
    if round_to is None:
        round_to = defaults.round_to
    round_to = int(round_to)
    if round_to < 1:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.round_to` must be >= 1"
        )
    min_gpus = gpu_sizing.min_gpus_per_job
    max_gpus = gpu_sizing.max_gpus_per_job
    if min_gpus < 1:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.min_gpus_per_job` must be >= 1"
        )
    if max_gpus is not None and max_gpus < min_gpus:
        raise ConfigContractError(
            f"`stages.{stage_name}.gpu_sizing.max_gpus_per_job` must be >= min_gpus_per_job"
        )

    usable_memory_per_gpu_gb = memory_gb * usable_memory_fraction
    required_memory_gb = target_memory_gb * safety_factor
    raw_total_gpus = max(1, ceil(required_memory_gb / usable_memory_per_gpu_gb))
    rounded_total_gpus = _ceil_to(raw_total_gpus, round_to)
    bounded_total_gpus = max(rounded_total_gpus, min_gpus)
    if max_gpus is not None:
        bounded_total_gpus = min(bounded_total_gpus, max_gpus)
    resolved_gpus_per_node = ceil(bounded_total_gpus / nodes)
    resolved_total_gpus = resolved_gpus_per_node * nodes
    if max_gpus_per_node not in (None, "") and resolved_gpus_per_node > int(
        max_gpus_per_node
    ):
        raise ConfigContractError(
            f"`stages.{stage_name}` resolves to {resolved_gpus_per_node} GPUs per node, "
            f"above hardware.gpu_types.{gpu_type}.max_gpus_per_node={max_gpus_per_node}"
        )

    return GpuSizingResolution(
        mode="auto",
        stage_name=stage_name,
        nodes=nodes,
        gpu_type=gpu_type,
        estimator=estimator,
        target_memory_gb=target_memory_gb,
        memory_gb=memory_gb,
        usable_memory_fraction=usable_memory_fraction,
        usable_memory_per_gpu_gb=usable_memory_per_gpu_gb,
        safety_factor=safety_factor,
        required_memory_gb=required_memory_gb,
        raw_total_gpus=raw_total_gpus,
        rounded_total_gpus=rounded_total_gpus,
        min_gpus_per_job=min_gpus,
        max_gpus_per_job=max_gpus,
        round_to=round_to,
        resolved_total_gpus=resolved_total_gpus,
        resolved_gpus_per_node=resolved_gpus_per_node,
    )
