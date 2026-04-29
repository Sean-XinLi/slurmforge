from __future__ import annotations

from dataclasses import dataclass

from ...config_contract.defaults import (
    DEFAULT_STAGE_RESOURCES_CPUS_PER_TASK,
    DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE,
    DEFAULT_STAGE_RESOURCES_NODES,
    DEFAULT_STAGE_RESOURCES_PARTITION,
    DEFAULT_STAGE_RESOURCES_TIME_LIMIT,
)


@dataclass(frozen=True)
class ResourceSpec:
    partition: str | None = DEFAULT_STAGE_RESOURCES_PARTITION
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = DEFAULT_STAGE_RESOURCES_TIME_LIMIT
    gpu_type: str = ""
    nodes: int = DEFAULT_STAGE_RESOURCES_NODES
    gpus_per_node: int | str = DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE
    cpus_per_task: int = DEFAULT_STAGE_RESOURCES_CPUS_PER_TASK
    mem: str | None = None
    constraint: str | None = None
    extra_sbatch_args: tuple[str, ...] = ()
