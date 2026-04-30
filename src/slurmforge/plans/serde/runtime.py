from __future__ import annotations

from typing import Any

from ...record_fields import (
    required_array,
    required_field,
    required_object,
    required_record,
    required_string,
    required_string_tuple,
)
from ..runtime import (
    EnvironmentPlan,
    EnvironmentSourcePlan,
    ExecutorRuntimePlan,
    PythonRuntimePlan,
    RuntimePlan,
    UserRuntimePlan,
)


def python_runtime_plan_from_dict(payload: dict[str, Any]) -> PythonRuntimePlan:
    return PythonRuntimePlan(
        bin=required_string(payload, "bin", label="python_runtime_plan", non_empty=True),
        min_version=required_string(
            payload, "min_version", label="python_runtime_plan"
        ),
    )


def executor_runtime_plan_from_dict(payload: dict[str, Any]) -> ExecutorRuntimePlan:
    return ExecutorRuntimePlan(
        python=python_runtime_plan_from_dict(
            required_object(payload, "python", label="executor_runtime_plan")
        ),
        module=required_string(
            payload, "module", label="executor_runtime_plan", non_empty=True
        ),
    )


def user_runtime_plan_from_dict(
    payload: dict[str, Any] | None,
) -> UserRuntimePlan | None:
    if payload is None:
        return None
    return UserRuntimePlan(
        name=required_string(payload, "name", label="user_runtime_plan", non_empty=True),
        python=python_runtime_plan_from_dict(
            required_object(payload, "python", label="user_runtime_plan")
        ),
        env=required_object(payload, "env", label="user_runtime_plan"),
    )


def runtime_plan_from_dict(payload: dict[str, Any]) -> RuntimePlan:
    user_payload = required_field(payload, "user", label="runtime_plan")
    if user_payload is not None:
        user_payload = required_record(user_payload, "runtime_plan.user")
    return RuntimePlan(
        executor=executor_runtime_plan_from_dict(
            required_object(payload, "executor", label="runtime_plan")
        ),
        user=user_runtime_plan_from_dict(user_payload),
    )


def environment_plan_from_dict(payload: dict[str, Any]) -> EnvironmentPlan:
    return EnvironmentPlan(
        name=required_string(payload, "name", label="environment_plan"),
        modules=required_string_tuple(payload, "modules", label="environment_plan"),
        source=tuple(
            _environment_source_from_dict(item)
            for item in required_array(payload, "source", label="environment_plan")
        ),
        env=required_object(payload, "env", label="environment_plan"),
    )


def _environment_source_from_dict(payload: Any) -> EnvironmentSourcePlan:
    record = required_record(payload, "environment_plan.source item")
    return EnvironmentSourcePlan(
        path=required_string(record, "path", label="environment_plan.source item"),
        args=required_string_tuple(record, "args", label="environment_plan.source item"),
    )
