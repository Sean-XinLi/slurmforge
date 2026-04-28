from __future__ import annotations

from ..errors import ConfigContractError
from .models import ExperimentSpec
from .validation_common import reject_newline


def validate_environment_contract(spec: ExperimentSpec) -> None:
    for name, environment in spec.environments.items():
        reject_newline(name, field=f"environments.{name}")
        for index, module in enumerate(environment.modules):
            if not module:
                raise ConfigContractError(f"`environments.{name}.modules[{index}]` must not be empty")
            reject_newline(module, field=f"environments.{name}.modules[{index}]")
        for index, source in enumerate(environment.source):
            field = f"environments.{name}.source[{index}]"
            if not source.path:
                raise ConfigContractError(f"`{field}.path` is required")
            reject_newline(source.path, field=f"{field}.path")
            for arg_index, arg in enumerate(source.args):
                reject_newline(arg, field=f"{field}.args[{arg_index}]")
        for key, value in environment.env.items():
            if not str(key):
                raise ConfigContractError(f"`environments.{name}.env` contains an empty key")
            reject_newline(str(key), field=f"environments.{name}.env")
            reject_newline(str(value), field=f"environments.{name}.env.{key}")


def validate_runtime_contract(spec: ExperimentSpec) -> None:
    if spec.orchestration.controller.cpus < 1:
        raise ConfigContractError("`orchestration.controller.cpus` must be >= 1")
    if not spec.runtime.executor.python.bin:
        raise ConfigContractError("`runtime.executor.python.bin` must not be empty")
    if not spec.runtime.executor.python.min_version:
        raise ConfigContractError("`runtime.executor.python.min_version` must not be empty")
    if not spec.runtime.executor.executor_module:
        raise ConfigContractError("`runtime.executor.module` must not be empty")
    validate_environment_contract(spec)
    if "default" not in spec.runtime.user:
        raise ConfigContractError("`runtime.user.default` is required")
    for runtime_name, runtime in spec.runtime.user.items():
        if not runtime.python.bin:
            raise ConfigContractError(f"`runtime.user.{runtime_name}.python.bin` must not be empty")
        if not runtime.python.min_version:
            raise ConfigContractError(f"`runtime.user.{runtime_name}.python.min_version` must not be empty")
    controller_environment = spec.orchestration.controller.environment
    if controller_environment and controller_environment not in spec.environments:
        raise ConfigContractError(
            "`orchestration.controller.environment` references unknown environment "
            f"`{controller_environment}`"
        )
