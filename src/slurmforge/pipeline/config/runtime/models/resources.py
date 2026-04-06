from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourcesConfig:
    auto_gpu: bool = True
    gpu_estimator: str = "heuristic"
    target_mem_per_gpu_gb: float = 40.0
    safety_factor: float = 1.15
    min_gpus_per_job: int = 1
    max_gpus_per_job: int = 8
    max_available_gpus: int = 8
