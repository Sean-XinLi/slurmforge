from __future__ import annotations

import copy
from typing import Any

from .models import GpuSizingDefaultsSpec, GpuTypeSpec, HardwareSpec, ResourceSpec, SizingSpec
from .parse_common import optional_mapping, reject_unknown_keys, require_mapping


def parse_resources(raw: Any, *, name: str) -> ResourceSpec:
    data = optional_mapping(raw, f"stages.{name}.resources")
    reject_unknown_keys(
        data,
        allowed={
            "partition",
            "account",
            "qos",
            "time_limit",
            "gpu_type",
            "nodes",
            "gpus_per_node",
            "cpus_per_task",
            "mem",
            "constraint",
            "extra_sbatch_args",
        },
        name=f"stages.{name}.resources",
    )
    extra = data.get("extra_sbatch_args") or ()
    if isinstance(extra, str):
        extra_args = (extra,)
    else:
        extra_args = tuple(str(item) for item in extra)
    raw_gpus_per_node = data.get("gpus_per_node", 0)
    if str(raw_gpus_per_node).lower() == "auto":
        gpus_per_node: int | str = "auto"
    else:
        gpus_per_node = int(raw_gpus_per_node or 0)
    return ResourceSpec(
        partition=None if data.get("partition") in (None, "") else str(data.get("partition")),
        account=None if data.get("account") in (None, "") else str(data.get("account")),
        qos=None if data.get("qos") in (None, "") else str(data.get("qos")),
        time_limit=None if data.get("time_limit") in (None, "") else str(data.get("time_limit")),
        gpu_type="" if data.get("gpu_type") in (None, "") else str(data.get("gpu_type")),
        nodes=int(data.get("nodes", 1)),
        gpus_per_node=gpus_per_node,
        cpus_per_task=int(data.get("cpus_per_task", 1)),
        mem=None if data.get("mem") in (None, "") else str(data.get("mem")),
        constraint=None if data.get("constraint") in (None, "") else str(data.get("constraint")),
        extra_sbatch_args=extra_args,
    )


def parse_gpu_type(name: str, raw: Any) -> GpuTypeSpec:
    data = require_mapping(raw, f"hardware.gpu_types.{name}")
    reject_unknown_keys(
        data,
        allowed={"memory_gb", "usable_memory_fraction", "max_gpus_per_node", "slurm"},
        name=f"hardware.gpu_types.{name}",
    )
    slurm = optional_mapping(data.get("slurm"), f"hardware.gpu_types.{name}.slurm")
    reject_unknown_keys(slurm, allowed={"constraint"}, name=f"hardware.gpu_types.{name}.slurm")
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
    reject_unknown_keys(data, allowed={"gpu_types"}, name="hardware")
    gpu_types_raw = optional_mapping(data.get("gpu_types"), "hardware.gpu_types")
    return HardwareSpec(
        gpu_types={
            str(name): parse_gpu_type(str(name), value)
            for name, value in sorted(gpu_types_raw.items())
        }
    )


def parse_sizing(raw: Any) -> SizingSpec:
    data = optional_mapping(raw, "sizing")
    reject_unknown_keys(data, allowed={"gpu"}, name="sizing")
    gpu = optional_mapping(data.get("gpu"), "sizing.gpu")
    reject_unknown_keys(gpu, allowed={"defaults"}, name="sizing.gpu")
    defaults = optional_mapping(gpu.get("defaults"), "sizing.gpu.defaults")
    reject_unknown_keys(defaults, allowed={"safety_factor", "round_to"}, name="sizing.gpu.defaults")
    return SizingSpec(
        gpu=GpuSizingDefaultsSpec(
            safety_factor=float(defaults.get("safety_factor", 1.0) or 1.0),
            round_to=int(defaults.get("round_to", 1) or 1),
        )
    )
