from __future__ import annotations

from typing import Any

from ..models import ResourcesConfig


def serialize_resources_config(config: ResourcesConfig) -> dict[str, Any]:
    return {
        "auto_gpu": bool(config.auto_gpu),
        "gpu_estimator": str(config.gpu_estimator),
        "target_mem_per_gpu_gb": float(config.target_mem_per_gpu_gb),
        "safety_factor": float(config.safety_factor),
        "min_gpus_per_job": int(config.min_gpus_per_job),
        "max_gpus_per_job": int(config.max_gpus_per_job),
        "max_available_gpus": int(config.max_available_gpus),
    }
