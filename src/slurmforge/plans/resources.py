from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourcePlan:
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    gpu_type: str = ""
    nodes: int = 1
    gpus_per_node: int = 0
    cpus_per_task: int = 1
    mem: str | None = None
    constraint: str | None = None
    extra_sbatch_args: tuple[str, ...] = ()

    @property
    def total_gpus(self) -> int:
        return self.nodes * self.gpus_per_node


@dataclass(frozen=True)
class ControlResourcesPlan:
    partition: str | None = None
    cpus: int = 1
    mem: str | None = None
    time_limit: str | None = None
