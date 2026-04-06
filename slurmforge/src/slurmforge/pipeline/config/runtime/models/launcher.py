from __future__ import annotations

from dataclasses import dataclass, field


def default_extra_torchrun_args() -> list[str]:
    return []


@dataclass(frozen=True)
class DistributedConfig:
    nnodes: int = 1
    nproc_per_node: int | None = None
    master_port: int = 29500
    port_offset: int | None = None
    extra_torchrun_args: list[str] = field(default_factory=default_extra_torchrun_args)


@dataclass(frozen=True)
class LauncherConfig:
    mode: str = "auto"
    python_bin: str = "python3"
    workdir: str = "."
    distributed: DistributedConfig = field(default_factory=DistributedConfig)
