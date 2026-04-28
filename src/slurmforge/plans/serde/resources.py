from __future__ import annotations

from typing import Any

from ...sizing.models import GpuSizingResolution
from ..resources import ControlResourcesPlan, ResourcePlan


def resource_plan_from_dict(payload: dict[str, Any]) -> ResourcePlan:
    return ResourcePlan(
        partition=None if payload["partition"] in (None, "") else str(payload["partition"]),
        account=None if payload["account"] in (None, "") else str(payload["account"]),
        qos=None if payload["qos"] in (None, "") else str(payload["qos"]),
        time_limit=None if payload["time_limit"] in (None, "") else str(payload["time_limit"]),
        gpu_type=str(payload["gpu_type"]),
        nodes=int(payload["nodes"]),
        gpus_per_node=int(payload["gpus_per_node"]),
        cpus_per_task=int(payload["cpus_per_task"]),
        mem=None if payload["mem"] in (None, "") else str(payload["mem"]),
        constraint=None if payload["constraint"] in (None, "") else str(payload["constraint"]),
        extra_sbatch_args=tuple(str(item) for item in payload["extra_sbatch_args"]),
    )


def resource_sizing_from_dict(payload: dict[str, Any]) -> GpuSizingResolution:
    return GpuSizingResolution(
        mode=str(payload["mode"]),
        stage_name=str(payload["stage_name"]),
        nodes=int(payload["nodes"]),
        gpu_type=str(payload.get("gpu_type") or ""),
        estimator=str(payload.get("estimator") or ""),
        target_memory_gb=None if payload.get("target_memory_gb") is None else float(payload["target_memory_gb"]),
        memory_gb=None if payload.get("memory_gb") is None else float(payload["memory_gb"]),
        usable_memory_fraction=None
        if payload.get("usable_memory_fraction") is None
        else float(payload["usable_memory_fraction"]),
        usable_memory_per_gpu_gb=None
        if payload.get("usable_memory_per_gpu_gb") is None
        else float(payload["usable_memory_per_gpu_gb"]),
        safety_factor=None if payload.get("safety_factor") is None else float(payload["safety_factor"]),
        required_memory_gb=None if payload.get("required_memory_gb") is None else float(payload["required_memory_gb"]),
        raw_total_gpus=None if payload.get("raw_total_gpus") is None else int(payload["raw_total_gpus"]),
        rounded_total_gpus=None if payload.get("rounded_total_gpus") is None else int(payload["rounded_total_gpus"]),
        min_gpus_per_job=None if payload.get("min_gpus_per_job") is None else int(payload["min_gpus_per_job"]),
        max_gpus_per_job=None if payload.get("max_gpus_per_job") is None else int(payload["max_gpus_per_job"]),
        round_to=int(payload["round_to"]),
        resolved_total_gpus=int(payload["resolved_total_gpus"]),
        resolved_gpus_per_node=int(payload["resolved_gpus_per_node"]),
    )


def control_resources_plan_from_dict(payload: dict[str, Any]) -> ControlResourcesPlan:
    return ControlResourcesPlan(
        partition=None if payload["partition"] in (None, "") else str(payload["partition"]),
        cpus=int(payload["cpus"]),
        mem=None if payload["mem"] in (None, "") else str(payload["mem"]),
        time_limit=None if payload["time_limit"] in (None, "") else str(payload["time_limit"]),
    )
