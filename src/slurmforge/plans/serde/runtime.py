from __future__ import annotations

from typing import Any

from ..runtime import (
    EnvironmentPlan,
    EnvironmentSourcePlan,
    ExecutorRuntimePlan,
    PythonRuntimePlan,
    RuntimePlan,
    UserRuntimePlan,
)


def python_runtime_plan_from_dict(payload: dict[str, Any]) -> PythonRuntimePlan:
    return PythonRuntimePlan(bin=str(payload["bin"]), min_version=str(payload["min_version"]))


def executor_runtime_plan_from_dict(payload: dict[str, Any]) -> ExecutorRuntimePlan:
    return ExecutorRuntimePlan(
        python=python_runtime_plan_from_dict(payload["python"]),
        module=str(payload["module"]),
    )


def user_runtime_plan_from_dict(payload: dict[str, Any] | None) -> UserRuntimePlan | None:
    if payload is None:
        return None
    return UserRuntimePlan(
        name=str(payload["name"]),
        python=python_runtime_plan_from_dict(payload["python"]),
        env=dict(payload["env"]),
    )


def runtime_plan_from_dict(payload: dict[str, Any]) -> RuntimePlan:
    return RuntimePlan(
        executor=executor_runtime_plan_from_dict(payload["executor"]),
        user=user_runtime_plan_from_dict(payload.get("user")),
    )


def environment_plan_from_dict(payload: dict[str, Any]) -> EnvironmentPlan:
    return EnvironmentPlan(
        name=str(payload["name"]),
        modules=tuple(str(item) for item in payload["modules"]),
        source=tuple(
            EnvironmentSourcePlan(path=str(item["path"]), args=tuple(str(arg) for arg in item["args"]))
            for item in payload["source"]
        ),
        env=dict(payload["env"]),
    )
