from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.registry import default_for

DEFAULT_CONTROL_CPUS = default_for("orchestration.control.cpus")
DEFAULT_CONTROL_ENVIRONMENT = default_for("orchestration.control.environment")
DEFAULT_CONTROL_MEM = default_for("orchestration.control.mem")
DEFAULT_CONTROL_PARTITION = default_for("orchestration.control.partition")
DEFAULT_CONTROL_TIME_LIMIT = default_for("orchestration.control.time_limit")
DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS = default_for("dispatch.max_available_gpus")
DEFAULT_DISPATCH_OVERFLOW_POLICY = default_for("dispatch.overflow_policy")


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
