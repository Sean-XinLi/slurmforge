from __future__ import annotations

import copy
from typing import Any

from ..config_contract.default_values import AUTO_VALUE
from ..config_contract.keys import reject_unknown_config_keys
from ..config_contract.registry import default_for
from .models import (
    GpuSizingDefaultsSpec,
    GpuTypeSpec,
    HardwareSpec,
    ResourceSpec,
    SizingSpec,
)
from .parse_common import optional_mapping, require_mapping

def parse_resources(raw: Any, *, name: str) -> ResourceSpec:
    data = optional_mapping(raw, f"stages.{name}.resources")
    reject_unknown_config_keys(data, parent=f"stages.{name}.resources")
    extra = data.get("extra_sbatch_args") or ()
    if isinstance(extra, str):
        extra_args = (extra,)
    else:
        extra_args = tuple(str(item) for item in extra)
    default_gpus_per_node = default_for("stages.*.resources.gpus_per_node")
    raw_gpus_per_node = data.get("gpus_per_node", default_gpus_per_node)
    if str(raw_gpus_per_node).lower() == AUTO_VALUE:
        gpus_per_node: int | str = AUTO_VALUE
    else:
        gpus_per_node = int(raw_gpus_per_node or 0)
    default_partition = default_for("stages.*.resources.partition")
    default_time_limit = default_for("stages.*.resources.time_limit")
    return ResourceSpec(
        partition=(
            default_partition
            if data.get("partition") in (None, "")
            else str(data.get("partition"))
        ),
        account=None if data.get("account") in (None, "") else str(data.get("account")),
        qos=None if data.get("qos") in (None, "") else str(data.get("qos")),
        time_limit=(
            default_time_limit
            if data.get("time_limit") in (None, "")
            else str(data.get("time_limit"))
        ),
        gpu_type=""
        if data.get("gpu_type") in (None, "")
        else str(data.get("gpu_type")),
        nodes=int(data.get("nodes", default_for("stages.*.resources.nodes"))),
        gpus_per_node=gpus_per_node,
        cpus_per_task=int(
            data.get(
                "cpus_per_task",
                default_for("stages.*.resources.cpus_per_task"),
            )
        ),
        mem=None if data.get("mem") in (None, "") else str(data.get("mem")),
        constraint=None
        if data.get("constraint") in (None, "")
        else str(data.get("constraint")),
        extra_sbatch_args=extra_args,
    )


def parse_gpu_type(name: str, raw: Any) -> GpuTypeSpec:
    data = require_mapping(raw, f"hardware.gpu_types.{name}")
    reject_unknown_config_keys(data, parent=f"hardware.gpu_types.{name}")
    slurm = optional_mapping(data.get("slurm"), f"hardware.gpu_types.{name}.slurm")
    reject_unknown_config_keys(slurm, parent=f"hardware.gpu_types.{name}.slurm")
    max_gpus = data.get("max_gpus_per_node")
    return GpuTypeSpec(
        name=name,
        memory_gb=float(data.get("memory_gb", 0) or 0),
        usable_memory_fraction=float(data.get("usable_memory_fraction", 0) or 0),
        max_gpus_per_node=None if max_gpus in (None, "") else int(max_gpus),
        slurm=copy.deepcopy(slurm),
    )


def parse_hardware(raw: Any) -> HardwareSpec:
    data = optional_mapping(raw, "hardware")
    reject_unknown_config_keys(data, parent="hardware")
    gpu_types_raw = optional_mapping(data.get("gpu_types"), "hardware.gpu_types")
    return HardwareSpec(
        gpu_types={
            str(name): parse_gpu_type(str(name), value)
            for name, value in sorted(gpu_types_raw.items())
        }
    )


def parse_sizing(raw: Any) -> SizingSpec:
    data = optional_mapping(raw, "sizing")
    reject_unknown_config_keys(data, parent="sizing")
    gpu = optional_mapping(data.get("gpu"), "sizing.gpu")
    reject_unknown_config_keys(gpu, parent="sizing.gpu")
    defaults = optional_mapping(gpu.get("defaults"), "sizing.gpu.defaults")
    reject_unknown_config_keys(defaults, parent="sizing.gpu.defaults")
    default_safety_factor = default_for("sizing.gpu.defaults.safety_factor")
    default_round_to = default_for("sizing.gpu.defaults.round_to")
    return SizingSpec(
        gpu=GpuSizingDefaultsSpec(
            safety_factor=float(
                defaults.get("safety_factor", default_safety_factor)
                or default_safety_factor
            ),
            round_to=int(
                defaults.get("round_to", default_round_to) or default_round_to
            ),
        )
    )
