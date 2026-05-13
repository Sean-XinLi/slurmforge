from __future__ import annotations

from dataclasses import dataclass

from ..io import SchemaVersion


@dataclass(frozen=True)
class GpuResourceSizingRequest:
    stage_name: str
    nodes: int
    gpus_per_node: int | str
    gpu_type: str = ""


@dataclass(frozen=True)
class GpuHardwareProfile:
    name: str
    memory_gb: float
    usable_memory_fraction: float
    max_gpus_per_node: int | None = None


@dataclass(frozen=True)
class GpuSizingDefaults:
    safety_factor: float = 1.0
    round_to: int = 1


@dataclass(frozen=True)
class StageGpuSizingPolicy:
    estimator: str
    target_memory_gb: float
    min_gpus_per_job: int = 1
    max_gpus_per_job: int | None = None
    safety_factor: float | None = None
    round_to: int | None = None


@dataclass(frozen=True)
class GpuSizingResolution:
    mode: str
    stage_name: str
    nodes: int
    gpu_type: str = ""
    estimator: str = ""
    target_memory_gb: float | None = None
    memory_gb: float | None = None
    usable_memory_fraction: float | None = None
    usable_memory_per_gpu_gb: float | None = None
    safety_factor: float | None = None
    required_memory_gb: float | None = None
    raw_total_gpus: int | None = None
    rounded_total_gpus: int | None = None
    min_gpus_per_job: int | None = None
    max_gpus_per_job: int | None = None
    round_to: int = 1
    resolved_total_gpus: int = 0
    resolved_gpus_per_node: int = 0
    schema_version: int = SchemaVersion.RESOURCE_SIZING
