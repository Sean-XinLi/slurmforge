from __future__ import annotations

from dataclasses import dataclass, field

from .common import JsonObject


@dataclass(frozen=True)
class PythonRuntimeSpec:
    bin: str = ""
    min_version: str = ""


@dataclass(frozen=True)
class ExecutorRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    executor_module: str = ""


@dataclass(frozen=True)
class UserRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    env: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeSpec:
    executor: ExecutorRuntimeSpec = field(default_factory=ExecutorRuntimeSpec)
    user: dict[str, UserRuntimeSpec] = field(default_factory=dict)
