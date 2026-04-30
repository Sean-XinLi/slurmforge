from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.defaults import (
    DEFAULT_CONTROL_CPUS,
    DEFAULT_CONTROL_ENVIRONMENT,
    DEFAULT_CONTROL_MEM,
    DEFAULT_CONTROL_PARTITION,
    DEFAULT_CONTROL_TIME_LIMIT,
    DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    DEFAULT_DISPATCH_OVERFLOW_POLICY,
)


@dataclass(frozen=True)
class DispatchSpec:
    max_available_gpus: int = DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS
    overflow_policy: str = DEFAULT_DISPATCH_OVERFLOW_POLICY


@dataclass(frozen=True)
class ControlSpec:
    partition: str | None = DEFAULT_CONTROL_PARTITION
    cpus: int = DEFAULT_CONTROL_CPUS
    mem: str | None = DEFAULT_CONTROL_MEM
    time_limit: str | None = DEFAULT_CONTROL_TIME_LIMIT
    environment: str = DEFAULT_CONTROL_ENVIRONMENT


@dataclass(frozen=True)
class OrchestrationSpec:
    control: ControlSpec = field(default_factory=ControlSpec)
