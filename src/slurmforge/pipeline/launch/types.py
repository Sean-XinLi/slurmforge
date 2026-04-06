from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShellToken:
    value: str
    raw: bool = False


@dataclass(frozen=True)
class LaunchRuntime:
    nnodes: int
    nproc_per_node: int
    master_port: int | None = None
