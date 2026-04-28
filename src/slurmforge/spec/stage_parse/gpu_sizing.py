from __future__ import annotations

from typing import Any

from ...errors import ConfigContractError
from ..models import StageGpuSizingSpec
from ..parse_common import reject_unknown_keys, require_mapping


def parse_stage_gpu_sizing(raw: Any, *, stage_name: str) -> StageGpuSizingSpec | None:
    if raw in (None, ""):
        return None
    data = require_mapping(raw, f"stages.{stage_name}.gpu_sizing")
    reject_unknown_keys(
        data,
        allowed={
            "estimator",
            "target_memory_gb",
            "min_gpus_per_job",
            "max_gpus_per_job",
            "safety_factor",
            "round_to",
        },
        name=f"stages.{stage_name}.gpu_sizing",
    )
    if data.get("estimator") in (None, ""):
        raise ConfigContractError(f"`stages.{stage_name}.gpu_sizing.estimator` is required")
    if data.get("target_memory_gb") in (None, ""):
        raise ConfigContractError(f"`stages.{stage_name}.gpu_sizing.target_memory_gb` is required")
    max_gpus = data.get("max_gpus_per_job")
    safety_factor = data.get("safety_factor")
    round_to = data.get("round_to")
    return StageGpuSizingSpec(
        estimator=str(data["estimator"]),
        target_memory_gb=float(data["target_memory_gb"]),
        min_gpus_per_job=int(data.get("min_gpus_per_job", 1) or 1),
        max_gpus_per_job=None if max_gpus in (None, "") else int(max_gpus),
        safety_factor=None if safety_factor in (None, "") else float(safety_factor),
        round_to=None if round_to in (None, "") else int(round_to),
    )
