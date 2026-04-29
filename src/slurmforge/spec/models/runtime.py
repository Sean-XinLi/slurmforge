from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.defaults import (
    DEFAULT_EXECUTOR_MODULE,
    DEFAULT_PYTHON_BIN,
    DEFAULT_PYTHON_MIN_VERSION,
    DEFAULT_RUNTIME_NAME,
)
from .common import JsonObject


@dataclass(frozen=True)
class PythonRuntimeSpec:
    bin: str = DEFAULT_PYTHON_BIN
    min_version: str = DEFAULT_PYTHON_MIN_VERSION


@dataclass(frozen=True)
class ExecutorRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    executor_module: str = DEFAULT_EXECUTOR_MODULE


@dataclass(frozen=True)
class UserRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    env: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeSpec:
    executor: ExecutorRuntimeSpec = field(default_factory=ExecutorRuntimeSpec)
    user: dict[str, UserRuntimeSpec] = field(
        default_factory=lambda: {DEFAULT_RUNTIME_NAME: UserRuntimeSpec()}
    )
