from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceSpec:
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    gpu_type: str = ""
    nodes: int = 0
    gpus_per_node: int | str = 0
    cpus_per_task: int = 0
    mem: str | None = None
    constraint: str | None = None
    extra_sbatch_args: tuple[str, ...] = ()
