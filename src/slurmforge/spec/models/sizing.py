from __future__ import annotations

from dataclasses import dataclass, field

from .common import JsonObject


@dataclass(frozen=True)
class GpuTypeSpec:
    name: str
    memory_gb: float
    usable_memory_fraction: float
    max_gpus_per_node: int | None = None
    slurm: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class HardwareSpec:
    gpu_types: dict[str, GpuTypeSpec] = field(default_factory=dict)


@dataclass(frozen=True)
class GpuSizingDefaultsSpec:
    safety_factor: float = 1.0
    round_to: int = 1


@dataclass(frozen=True)
class SizingSpec:
    gpu: GpuSizingDefaultsSpec = field(default_factory=GpuSizingDefaultsSpec)


@dataclass(frozen=True)
class StageGpuSizingSpec:
    estimator: str
    target_memory_gb: float
    min_gpus_per_job: int = 1
    max_gpus_per_job: int | None = None
    safety_factor: float | None = None
    round_to: int | None = None
