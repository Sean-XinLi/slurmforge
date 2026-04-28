from __future__ import annotations

import copy

from ...plans.runtime import (
    EnvironmentPlan,
    EnvironmentSourcePlan,
    ExecutorRuntimePlan,
    PythonRuntimePlan,
    RuntimePlan,
    UserRuntimePlan,
)
from ...spec import ExperimentSpec, StageSpec


def executor_runtime_payload(spec: ExperimentSpec) -> ExecutorRuntimePlan:
    return ExecutorRuntimePlan(
        python=PythonRuntimePlan(
            bin=spec.runtime.executor.python.bin,
            min_version=spec.runtime.executor.python.min_version,
        ),
        module=spec.runtime.executor.executor_module,
    )


def runtime_payload(spec: ExperimentSpec, stage: StageSpec) -> RuntimePlan:
    user_runtime = spec.runtime.user[stage.runtime]
    return RuntimePlan(
        executor=executor_runtime_payload(spec),
        user=UserRuntimePlan(
            name=stage.runtime,
            python=PythonRuntimePlan(
                bin=user_runtime.python.bin,
                min_version=user_runtime.python.min_version,
            ),
            env=copy.deepcopy(user_runtime.env),
        ),
    )


def environment_payload(spec: ExperimentSpec, name: str) -> EnvironmentPlan:
    if not name:
        return EnvironmentPlan()
    environment = spec.environments[name]
    return EnvironmentPlan(
        name=environment.name,
        modules=environment.modules,
        source=tuple(EnvironmentSourcePlan(path=source.path, args=source.args) for source in environment.source),
        env=copy.deepcopy(environment.env),
    )
