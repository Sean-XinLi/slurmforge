from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BeforeStepPlan:
    run: str
    name: str = ""


@dataclass(frozen=True)
class RendezvousPlan:
    backend: str = "c10d"
    endpoint: str = "auto"
    port: int = 29500


@dataclass(frozen=True)
class LauncherPlan:
    type: str = "single"
    mode: str = ""
    nnodes: int | None = None
    nproc_per_node: int | None = None
    rendezvous: RendezvousPlan | None = None
    args: tuple[str, ...] = ()
    srun_args: tuple[str, ...] = ()
    master_port: int | None = None
