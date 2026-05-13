from __future__ import annotations

from typing import Any

from ...io import SchemaVersion, require_schema
from ...record_fields import (
    required_int,
    required_nullable_float,
    required_nullable_int,
    required_nullable_string,
    required_string,
    required_string_tuple,
)
from ...sizing.models import GpuSizingResolution
from ..resources import ControlResourcesPlan, ResourcePlan


def resource_plan_from_dict(payload: dict[str, Any]) -> ResourcePlan:
    return ResourcePlan(
        partition=required_nullable_string(payload, "partition", label="resource_plan"),
        account=required_nullable_string(payload, "account", label="resource_plan"),
        qos=required_nullable_string(payload, "qos", label="resource_plan"),
        time_limit=required_nullable_string(
            payload, "time_limit", label="resource_plan"
        ),
        gpu_type=required_string(payload, "gpu_type", label="resource_plan"),
        nodes=required_int(payload, "nodes", label="resource_plan"),
        gpus_per_node=required_int(
            payload, "gpus_per_node", label="resource_plan"
        ),
        cpus_per_task=required_int(
            payload, "cpus_per_task", label="resource_plan"
        ),
        mem=required_nullable_string(payload, "mem", label="resource_plan"),
        constraint=required_nullable_string(
            payload, "constraint", label="resource_plan"
        ),
        extra_sbatch_args=required_string_tuple(
            payload, "extra_sbatch_args", label="resource_plan"
        ),
    )


def resource_sizing_from_dict(payload: dict[str, Any]) -> GpuSizingResolution:
    require_schema(payload, name="resource_sizing", version=SchemaVersion.RESOURCE_SIZING)
    return GpuSizingResolution(
        mode=required_string(
            payload, "mode", label="resource_sizing", non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label="resource_sizing", non_empty=True
        ),
        nodes=required_int(payload, "nodes", label="resource_sizing"),
        gpu_type=required_string(payload, "gpu_type", label="resource_sizing"),
        estimator=required_string(payload, "estimator", label="resource_sizing"),
        target_memory_gb=required_nullable_float(
            payload, "target_memory_gb", label="resource_sizing"
        ),
        memory_gb=required_nullable_float(
            payload, "memory_gb", label="resource_sizing"
        ),
        usable_memory_fraction=required_nullable_float(
            payload, "usable_memory_fraction", label="resource_sizing"
        ),
        usable_memory_per_gpu_gb=required_nullable_float(
            payload, "usable_memory_per_gpu_gb", label="resource_sizing"
        ),
        safety_factor=required_nullable_float(
            payload, "safety_factor", label="resource_sizing"
        ),
        required_memory_gb=required_nullable_float(
            payload, "required_memory_gb", label="resource_sizing"
        ),
        raw_total_gpus=required_nullable_int(
            payload, "raw_total_gpus", label="resource_sizing"
        ),
        rounded_total_gpus=required_nullable_int(
            payload, "rounded_total_gpus", label="resource_sizing"
        ),
        min_gpus_per_job=required_nullable_int(
            payload, "min_gpus_per_job", label="resource_sizing"
        ),
        max_gpus_per_job=required_nullable_int(
            payload, "max_gpus_per_job", label="resource_sizing"
        ),
        round_to=required_int(payload, "round_to", label="resource_sizing"),
        resolved_total_gpus=required_int(
            payload, "resolved_total_gpus", label="resource_sizing"
        ),
        resolved_gpus_per_node=required_int(
            payload, "resolved_gpus_per_node", label="resource_sizing"
        ),
    )


def control_resources_plan_from_dict(payload: dict[str, Any]) -> ControlResourcesPlan:
    return ControlResourcesPlan(
        partition=required_nullable_string(
            payload, "partition", label="control_resources_plan"
        ),
        cpus=required_int(payload, "cpus", label="control_resources_plan"),
        mem=required_nullable_string(payload, "mem", label="control_resources_plan"),
        time_limit=required_nullable_string(
            payload, "time_limit", label="control_resources_plan"
        ),
    )
