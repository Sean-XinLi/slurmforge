from __future__ import annotations

import copy
from typing import Any

from ..config_contract.default_values import (
    DEFAULT_ENVIRONMENT_NAME,
    DEFAULT_RUNTIME_NAME,
)
from ..config_contract.keys import reject_unknown_config_keys
from ..config_contract.registry import default_for
from ..errors import ConfigContractError
from .models import (
    ControlSpec,
    EnvironmentSourceSpec,
    EnvironmentSpec,
    ExecutorRuntimeSpec,
    OrchestrationSpec,
    PythonRuntimeSpec,
    RuntimeSpec,
    UserRuntimeSpec,
)
from .parse_common import optional_mapping, require_mapping

def parse_python_runtime(raw: Any, *, name: str) -> PythonRuntimeSpec:
    data = require_mapping(raw, name)
    reject_unknown_config_keys(data, parent=name)
    if data.get("bin") in (None, ""):
        raise ConfigContractError(f"`{name}.bin` is required")
    return PythonRuntimeSpec(
        bin=str(data["bin"]),
        min_version=str(
            data.get("min_version")
            or default_for("runtime.executor.python.min_version")
        ),
    )


def parse_executor_runtime(raw: Any) -> ExecutorRuntimeSpec:
    data = require_mapping(raw, "runtime.executor")
    reject_unknown_config_keys(data, parent="runtime.executor")
    return ExecutorRuntimeSpec(
        python=parse_python_runtime(data.get("python"), name="runtime.executor.python"),
        executor_module=str(
            data.get("module") or default_for("runtime.executor.module")
        ),
    )


def parse_user_runtime(raw: Any, *, name: str) -> UserRuntimeSpec:
    data = require_mapping(raw, name)
    reject_unknown_config_keys(data, parent=name)
    return UserRuntimeSpec(
        python=parse_python_runtime(data.get("python"), name=f"{name}.python"),
        env=copy.deepcopy(optional_mapping(data.get("env"), f"{name}.env")),
    )


def parse_runtime(raw: Any) -> RuntimeSpec:
    data = require_mapping(raw, "runtime")
    reject_unknown_config_keys(data, parent="runtime")
    user_raw = require_mapping(data.get("user"), "runtime.user")
    user = {
        str(name): parse_user_runtime(value, name=f"runtime.user.{name}")
        for name, value in sorted(user_raw.items())
    }
    if DEFAULT_RUNTIME_NAME not in user:
        raise ConfigContractError(f"`runtime.user.{DEFAULT_RUNTIME_NAME}` is required")
    return RuntimeSpec(
        executor=parse_executor_runtime(data.get("executor")),
        user=user,
    )


def parse_environment_source(raw: Any, *, name: str) -> EnvironmentSourceSpec:
    data = require_mapping(raw, name)
    reject_unknown_config_keys(data, parent=name)
    if data.get("path") in (None, ""):
        raise ConfigContractError(f"`{name}.path` is required")
    args = data.get("args")
    if args is None:
        args = []
    if not isinstance(args, list):
        raise ConfigContractError(f"`{name}.args` must be a list")
    return EnvironmentSourceSpec(
        path=str(data["path"]),
        args=tuple(str(item) for item in args),
    )


def parse_environments(raw: Any) -> dict[str, EnvironmentSpec]:
    data = optional_mapping(raw, "environments")
    parsed: dict[str, EnvironmentSpec] = {}
    for env_name, env_raw in sorted(data.items()):
        env_data = require_mapping(env_raw, f"environments.{env_name}")
        reject_unknown_config_keys(env_data, parent=f"environments.{env_name}")
        modules = env_data.get("modules")
        if modules is None:
            modules = []
        if not isinstance(modules, list):
            raise ConfigContractError(
                f"`environments.{env_name}.modules` must be a list"
            )
        source_raw = env_data.get("source")
        if source_raw is None:
            source_raw = []
        if not isinstance(source_raw, list):
            raise ConfigContractError(
                f"`environments.{env_name}.source` must be a list"
            )
        parsed[str(env_name)] = EnvironmentSpec(
            name=str(env_name),
            modules=tuple(str(item) for item in modules),
            source=tuple(
                parse_environment_source(
                    item, name=f"environments.{env_name}.source[{index}]"
                )
                for index, item in enumerate(source_raw)
            ),
            env=copy.deepcopy(
                optional_mapping(env_data.get("env"), f"environments.{env_name}.env")
            ),
        )
    if DEFAULT_ENVIRONMENT_NAME not in parsed:
        parsed[DEFAULT_ENVIRONMENT_NAME] = EnvironmentSpec(
            name=DEFAULT_ENVIRONMENT_NAME
        )
    return parsed


def parse_orchestration(raw: Any) -> OrchestrationSpec:
    data = optional_mapping(raw, "orchestration")
    reject_unknown_config_keys(data, parent="orchestration")
    control = optional_mapping(data.get("control"), "orchestration.control")
    reject_unknown_config_keys(control, parent="orchestration.control")
    default_control_partition = default_for("orchestration.control.partition")
    default_control_mem = default_for("orchestration.control.mem")
    default_control_time_limit = default_for("orchestration.control.time_limit")
    default_control_environment = default_for("orchestration.control.environment")
    return OrchestrationSpec(
        control=ControlSpec(
            partition=(
                default_control_partition
                if control.get("partition") in (None, "")
                else str(control.get("partition"))
            ),
            cpus=int(control.get("cpus", default_for("orchestration.control.cpus"))),
            mem=(
                default_control_mem
                if control.get("mem") in (None, "")
                else str(control.get("mem"))
            ),
            time_limit=(
                default_control_time_limit
                if control.get("time_limit") in (None, "")
                else str(control.get("time_limit"))
            ),
            environment=(
                default_control_environment
                if control.get("environment") in (None, "")
                else str(control.get("environment"))
            ),
        )
    )
