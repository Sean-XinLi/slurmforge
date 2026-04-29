from __future__ import annotations

from ..errors import ConfigContractError
from .models import ExperimentSpec
from .validation_common import reject_newline


def validate_hardware_contract(spec: ExperimentSpec) -> None:
    for name, gpu_type in spec.hardware.gpu_types.items():
        reject_newline(name, field=f"hardware.gpu_types.{name}")
        if gpu_type.memory_gb <= 0:
            raise ConfigContractError(
                f"`hardware.gpu_types.{name}.memory_gb` must be > 0"
            )
        if gpu_type.usable_memory_fraction <= 0 or gpu_type.usable_memory_fraction > 1:
            raise ConfigContractError(
                f"`hardware.gpu_types.{name}.usable_memory_fraction` must be in (0, 1]"
            )
        if gpu_type.max_gpus_per_node is not None and gpu_type.max_gpus_per_node < 1:
            raise ConfigContractError(
                f"`hardware.gpu_types.{name}.max_gpus_per_node` must be >= 1"
            )
        for key, value in gpu_type.slurm.items():
            reject_newline(str(key), field=f"hardware.gpu_types.{name}.slurm")
            reject_newline(str(value), field=f"hardware.gpu_types.{name}.slurm.{key}")


def validate_sizing_contract(spec: ExperimentSpec) -> None:
    if spec.sizing.gpu.safety_factor < 1:
        raise ConfigContractError("`sizing.gpu.defaults.safety_factor` must be >= 1")
    if spec.sizing.gpu.round_to < 1:
        raise ConfigContractError("`sizing.gpu.defaults.round_to` must be >= 1")
