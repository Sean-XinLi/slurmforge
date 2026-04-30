from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.default_values import DEFAULT_RUNTIME_NAME
from ...config_contract.registry import default_for
from .common import JsonObject

DEFAULT_EXECUTOR_MODULE = default_for("runtime.executor.module")
DEFAULT_PYTHON_BIN = default_for("runtime.executor.python.bin")
DEFAULT_PYTHON_MIN_VERSION = default_for("runtime.executor.python.min_version")


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
