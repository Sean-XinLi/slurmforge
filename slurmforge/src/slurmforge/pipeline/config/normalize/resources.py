from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from ..scalars import normalize_bool
from ..runtime import DEFAULT_RESOURCES, ResourcesConfig
from ..utils import _warn, ensure_dict
from .shared import ensure_normalized_config


def normalize_resources(cfg: dict[str, Any]) -> ResourcesConfig:
    merged = deep_merge(DEFAULT_RESOURCES, ensure_dict(cfg, "resources"))

    max_available = int(merged.get("max_available_gpus", merged.get("max_gpus_per_job", 8)))
    if max_available < 1:
        raise ConfigContractError("resources.max_available_gpus must be >= 1")
    merged["max_available_gpus"] = max_available

    max_per_job = int(merged.get("max_gpus_per_job", max_available))
    if max_per_job < 1:
        raise ConfigContractError("resources.max_gpus_per_job must be >= 1")
    if max_per_job > max_available:
        _warn(
            "resources.max_gpus_per_job is capped by resources.max_available_gpus "
            f"({max_per_job} -> {max_available})"
        )
    merged["max_gpus_per_job"] = min(max_per_job, max_available)

    min_per_job = int(merged.get("min_gpus_per_job", 1))
    if min_per_job < 1:
        raise ConfigContractError("resources.min_gpus_per_job must be >= 1")
    if min_per_job > merged["max_gpus_per_job"]:
        _warn(
            "resources.min_gpus_per_job is capped by resources.max_gpus_per_job "
            f"({min_per_job} -> {merged['max_gpus_per_job']})"
        )
    merged["min_gpus_per_job"] = min(min_per_job, merged["max_gpus_per_job"])

    return ResourcesConfig(
        auto_gpu=normalize_bool(merged["auto_gpu"], name="resources.auto_gpu"),
        gpu_estimator=str(merged["gpu_estimator"]),
        target_mem_per_gpu_gb=float(merged["target_mem_per_gpu_gb"]),
        safety_factor=float(merged["safety_factor"]),
        min_gpus_per_job=int(merged["min_gpus_per_job"]),
        max_gpus_per_job=int(merged["max_gpus_per_job"]),
        max_available_gpus=int(merged["max_available_gpus"]),
    )


def ensure_resources_config(value: Any, name: str = "resources") -> ResourcesConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=ResourcesConfig,
        normalizer=normalize_resources,
    )
