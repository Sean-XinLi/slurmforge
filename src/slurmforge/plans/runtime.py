from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PythonRuntimePlan:
    bin: str
    min_version: str


@dataclass(frozen=True)
class ExecutorRuntimePlan:
    python: PythonRuntimePlan
    module: str


@dataclass(frozen=True)
class UserRuntimePlan:
    name: str
    python: PythonRuntimePlan
    env: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimePlan:
    executor: ExecutorRuntimePlan
    user: UserRuntimePlan | None = None


@dataclass(frozen=True)
class EnvironmentSourcePlan:
    path: str
    args: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnvironmentPlan:
    name: str = ""
    modules: tuple[str, ...] = ()
    source: tuple[EnvironmentSourcePlan, ...] = ()
    env: dict[str, Any] = field(default_factory=dict)
