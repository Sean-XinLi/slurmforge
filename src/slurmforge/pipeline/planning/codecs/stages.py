from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ...config.normalize import ensure_cluster_config, ensure_launcher_config
from ...config.runtime import serialize_cluster_config, serialize_launcher_config
from ..enums import RuntimeProbe
from ..models.stages import StageCapabilities, StageExecutionPlan
from .diagnostics import parse_plan_diagnostic, serialize_plan_diagnostic
from .resources import (
    parse_allocation_request,
    parse_execution_topology,
    parse_resource_estimate,
    serialize_allocation_request,
    serialize_execution_topology,
    serialize_resource_estimate,
)


def parse_stage_capabilities(value: Any, *, name: str = "capabilities") -> StageCapabilities:
    if isinstance(value, StageCapabilities):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return StageCapabilities(
        ddp_supported=bool(value.get("ddp_supported", False)),
        ddp_required=bool(value.get("ddp_required", False)),
        uses_gpu=bool(value.get("uses_gpu", True)),
        external_launcher=bool(value.get("external_launcher", False)),
        runtime_probe=value.get("runtime_probe", RuntimeProbe.CUDA.value),
    )


def serialize_stage_capabilities(capabilities: StageCapabilities) -> dict[str, Any]:
    return {
        "ddp_supported": bool(capabilities.ddp_supported),
        "ddp_required": bool(capabilities.ddp_required),
        "uses_gpu": bool(capabilities.uses_gpu),
        "external_launcher": bool(capabilities.external_launcher),
        "runtime_probe": capabilities.runtime_probe.value,
    }


def parse_stage_execution_plan(value: Any, *, name: str = "stage_plan") -> StageExecutionPlan:
    if isinstance(value, StageExecutionPlan):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return StageExecutionPlan(
        name=str(value.get("name", "") or ""),
        stage_kind=value.get("stage_kind", ""),
        invocation_kind=value.get("invocation_kind", ""),
        launcher_kind=value.get("launcher_kind", ""),
        command_text=str(value.get("command_text", "") or ""),
        workdir=Path(str(value.get("workdir", ".") or ".")),
        topology=parse_execution_topology(value.get("topology", {}), name=f"{name}.topology"),
        allocation=parse_allocation_request(value.get("allocation", {}), name=f"{name}.allocation"),
        estimate=parse_resource_estimate(value.get("estimate", {}), name=f"{name}.estimate"),
        capabilities=parse_stage_capabilities(value.get("capabilities", {}), name=f"{name}.capabilities"),
        python_bin=str(value.get("python_bin", "python3") or "python3"),
        launcher_cfg=None if value.get("launcher_cfg") is None else ensure_launcher_config(value.get("launcher_cfg")),
        cluster_cfg=None if value.get("cluster_cfg") is None else ensure_cluster_config(value.get("cluster_cfg")),
        script_path=None if value.get("script_path") in (None, "") else Path(str(value.get("script_path"))),
        cli_args=copy.deepcopy(value.get("cli_args") or {}),
        command_mode=None if value.get("command_mode") in (None, "") else str(value.get("command_mode")),
        requested_launcher_mode=None
        if value.get("requested_launcher_mode") in (None, "")
        else str(value.get("requested_launcher_mode")),
        max_gpus_per_job=int(value.get("max_gpus_per_job", 0) or 0),
        diagnostics=tuple(
            parse_plan_diagnostic(item, name=f"{name}.diagnostics[]") for item in list(value.get("diagnostics") or [])
        ),
    )


def serialize_stage_execution_plan(plan: StageExecutionPlan) -> dict[str, Any]:
    return {
        "name": plan.name,
        "stage_kind": plan.stage_kind.value,
        "invocation_kind": plan.invocation_kind.value,
        "launcher_kind": plan.launcher_kind.value,
        "command_text": plan.command_text,
        "workdir": str(plan.workdir),
        "topology": serialize_execution_topology(plan.topology),
        "allocation": serialize_allocation_request(plan.allocation),
        "estimate": serialize_resource_estimate(plan.estimate),
        "capabilities": serialize_stage_capabilities(plan.capabilities),
        "python_bin": plan.python_bin,
        "launcher_cfg": None if plan.launcher_cfg is None else serialize_launcher_config(plan.launcher_cfg),
        "cluster_cfg": None if plan.cluster_cfg is None else serialize_cluster_config(plan.cluster_cfg),
        "script_path": None if plan.script_path is None else str(plan.script_path),
        "cli_args": copy.deepcopy(plan.cli_args),
        "command_mode": plan.command_mode,
        "requested_launcher_mode": plan.requested_launcher_mode,
        "max_gpus_per_job": plan.max_gpus_per_job,
        "diagnostics": [serialize_plan_diagnostic(item) for item in plan.diagnostics],
    }
