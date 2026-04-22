from __future__ import annotations

import copy
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from ....errors import PlanningError
from ...config.normalize import ensure_cluster_config, ensure_launcher_config
from ...config.runtime import ClusterConfig, LauncherConfig
from ...launch.types import LaunchRuntime
from ..enums import InvocationKind, LauncherKind, RuntimeProbe, StageKind, coerce_enum
from .diagnostics import PlanDiagnostic, coerce_plan_diagnostic
from .resources import AllocationRequest, ExecutionTopology, ResourceEstimate


@dataclass(frozen=True)
class StageCapabilities:
    ddp_supported: bool
    ddp_required: bool
    uses_gpu: bool = True
    external_launcher: bool = False
    runtime_probe: RuntimeProbe = RuntimeProbe.CUDA

    def __post_init__(self) -> None:
        runtime_probe = coerce_enum(RuntimeProbe, self.runtime_probe, field_name="StageCapabilities.runtime_probe")
        object.__setattr__(self, "runtime_probe", runtime_probe)


@dataclass(frozen=True)
class StageExecutionPlan:
    name: str
    stage_kind: StageKind
    invocation_kind: InvocationKind
    launcher_kind: LauncherKind
    command_text: str
    workdir: Path
    topology: ExecutionTopology
    allocation: AllocationRequest
    estimate: ResourceEstimate
    capabilities: StageCapabilities
    python_bin: str = "python3"
    launcher_cfg: LauncherConfig | None = None
    cluster_cfg: ClusterConfig | None = None
    script_path: Path | None = None
    cli_args: dict[str, Any] = field(default_factory=dict)
    command_mode: str | None = None
    requested_launcher_mode: str | None = None
    max_gpus_per_job: int = 0
    diagnostics: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        name = str(self.name or "").strip()
        stage_kind = coerce_enum(StageKind, self.stage_kind, field_name="StageExecutionPlan.stage_kind")
        invocation_kind = coerce_enum(
            InvocationKind,
            self.invocation_kind,
            field_name="StageExecutionPlan.invocation_kind",
        )
        launcher_kind = coerce_enum(LauncherKind, self.launcher_kind, field_name="StageExecutionPlan.launcher_kind")
        command_text = str(self.command_text or "").strip()
        python_bin = str(self.python_bin or "python3").strip()
        if not name:
            raise PlanningError("StageExecutionPlan.name must be non-empty")
        if not command_text:
            raise PlanningError("StageExecutionPlan.command_text must be non-empty")
        if not python_bin:
            raise PlanningError("StageExecutionPlan.python_bin must be non-empty")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "stage_kind", stage_kind)
        object.__setattr__(self, "invocation_kind", invocation_kind)
        object.__setattr__(self, "launcher_kind", launcher_kind)
        object.__setattr__(self, "command_text", command_text)
        object.__setattr__(self, "workdir", Path(self.workdir).expanduser().resolve())
        object.__setattr__(self, "python_bin", python_bin)
        object.__setattr__(self, "launcher_cfg", None if self.launcher_cfg is None else ensure_launcher_config(self.launcher_cfg))
        object.__setattr__(self, "cluster_cfg", None if self.cluster_cfg is None else ensure_cluster_config(self.cluster_cfg))
        object.__setattr__(self, "script_path", None if self.script_path is None else Path(self.script_path).expanduser().resolve())
        object.__setattr__(self, "cli_args", copy.deepcopy(dict(self.cli_args or {})))
        object.__setattr__(self, "command_mode", None if self.command_mode is None else str(self.command_mode))
        object.__setattr__(self, "requested_launcher_mode", None if self.requested_launcher_mode is None else str(self.requested_launcher_mode))
        object.__setattr__(self, "max_gpus_per_job", int(self.max_gpus_per_job or 0))
        object.__setattr__(
            self,
            "diagnostics",
            tuple(coerce_plan_diagnostic(item, name=f"{name}.diagnostics[]") for item in self.diagnostics),
        )

    @property
    def runtime(self) -> LaunchRuntime:
        return self.topology.to_launch_runtime()

    def with_diagnostics(self, diagnostics: list[PlanDiagnostic] | tuple[PlanDiagnostic, ...]) -> StageExecutionPlan:
        return replace(self, diagnostics=tuple(diagnostics))
