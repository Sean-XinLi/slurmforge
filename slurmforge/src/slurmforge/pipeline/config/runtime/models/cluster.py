from __future__ import annotations

from dataclasses import dataclass, field


def default_extra_sbatch_args() -> list[str]:
    return []


@dataclass(frozen=True)
class ClusterConfig:
    partition: str = "b200"
    account: str = "YOUR_ACCOUNT"
    qos: str = "normal"
    time_limit: str = "3-00:00:00"
    nodes: int = 1
    gpus_per_node: int | None = None
    cpus_per_task: int = 8
    mem: str = "0"
    constraint: str = ""
    extra_sbatch_args: list[str] = field(default_factory=default_extra_sbatch_args)
