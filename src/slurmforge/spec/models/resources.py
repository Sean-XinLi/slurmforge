from __future__ import annotations

from dataclasses import dataclass

from ...config_contract.registry import default_for

DEFAULT_STAGE_RESOURCES_CPUS_PER_TASK = default_for("stages.*.resources.cpus_per_task")
DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE = default_for("stages.*.resources.gpus_per_node")
DEFAULT_STAGE_RESOURCES_NODES = default_for("stages.*.resources.nodes")
DEFAULT_STAGE_RESOURCES_PARTITION = default_for("stages.*.resources.partition")
DEFAULT_STAGE_RESOURCES_TIME_LIMIT = default_for("stages.*.resources.time_limit")


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
