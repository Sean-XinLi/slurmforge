from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DispatchSpec:
    max_available_gpus: int = 0
    overflow_policy: str = ""
    release_policy: str = ""
    window_size: int = 1
    window_seconds: int = 0


@dataclass(frozen=True)
class ControlSpec:
    partition: str | None = None
    cpus: int = 0
    mem: str | None = None
    time_limit: str | None = None
    environment: str = ""


@dataclass(frozen=True)
class OrchestrationSpec:
    control: ControlSpec = field(default_factory=ControlSpec)
