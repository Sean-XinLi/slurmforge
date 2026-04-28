from __future__ import annotations

from ..sizing.models import (
    GpuHardwareProfile,
    GpuResourceSizingRequest,
    GpuSizingDefaults,
    StageGpuSizingPolicy,
)
from .models import ExperimentSpec, StageSpec


def stage_gpu_sizing_inputs(
    spec: ExperimentSpec,
    stage: StageSpec,
) -> tuple[
    GpuResourceSizingRequest,
    StageGpuSizingPolicy | None,
    dict[str, GpuHardwareProfile],
    GpuSizingDefaults,
]:
    resources = stage.resources
    request = GpuResourceSizingRequest(
        stage_name=stage.name,
        nodes=resources.nodes,
        gpus_per_node=resources.gpus_per_node,
        gpu_type=resources.gpu_type,
    )
    policy = None
    if stage.gpu_sizing is not None:
        policy = StageGpuSizingPolicy(
            estimator=stage.gpu_sizing.estimator,
            target_memory_gb=stage.gpu_sizing.target_memory_gb,
            min_gpus_per_job=stage.gpu_sizing.min_gpus_per_job,
            max_gpus_per_job=stage.gpu_sizing.max_gpus_per_job,
            safety_factor=stage.gpu_sizing.safety_factor,
            round_to=stage.gpu_sizing.round_to,
        )
    profiles = {
        name: GpuHardwareProfile(
            name=gpu_type.name,
            memory_gb=gpu_type.memory_gb,
            usable_memory_fraction=gpu_type.usable_memory_fraction,
            max_gpus_per_node=gpu_type.max_gpus_per_node,
        )
        for name, gpu_type in spec.hardware.gpu_types.items()
    }
    defaults = GpuSizingDefaults(
        safety_factor=spec.sizing.gpu.safety_factor,
        round_to=spec.sizing.gpu.round_to,
    )
    return request, policy, profiles, defaults
