from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DispatchSpec:
    max_available_gpus: int = 0
    overflow_policy: str = "serialize_groups"


@dataclass(frozen=True)
class ControllerSpec:
    partition: str | None = None
    cpus: int = 1
    mem: str | None = "2G"
    time_limit: str | None = None
    environment: str = ""


@dataclass(frozen=True)
class OrchestrationSpec:
    controller: ControllerSpec = field(default_factory=ControllerSpec)
