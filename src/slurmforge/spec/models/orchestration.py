from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.defaults import (
    DEFAULT_CONTROLLER_CPUS,
    DEFAULT_CONTROLLER_ENVIRONMENT,
    DEFAULT_CONTROLLER_MEM,
    DEFAULT_CONTROLLER_TIME_LIMIT,
    DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    DEFAULT_DISPATCH_OVERFLOW_POLICY,
    DEFAULT_STAGE_RESOURCES_PARTITION,
)


@dataclass(frozen=True)
class DispatchSpec:
    max_available_gpus: int = DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS
    overflow_policy: str = DEFAULT_DISPATCH_OVERFLOW_POLICY


@dataclass(frozen=True)
class ControllerSpec:
    partition: str | None = DEFAULT_STAGE_RESOURCES_PARTITION
    cpus: int = DEFAULT_CONTROLLER_CPUS
    mem: str | None = DEFAULT_CONTROLLER_MEM
    time_limit: str | None = DEFAULT_CONTROLLER_TIME_LIMIT
    environment: str = DEFAULT_CONTROLLER_ENVIRONMENT


@dataclass(frozen=True)
class OrchestrationSpec:
    controller: ControllerSpec = field(default_factory=ControllerSpec)
