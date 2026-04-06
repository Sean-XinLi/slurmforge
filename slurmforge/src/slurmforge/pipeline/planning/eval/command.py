from __future__ import annotations

from pathlib import Path

from ....errors import PlanningError
from ...config.api import EvalConfigSpec
from ...config.runtime import ValidationConfig
from ..contracts import ExecutionTopology, StageCapabilities, StageExecutionPlan
from ..enums import InvocationKind, LauncherKind, StageKind
from ..external_command import resolve_external_command_text
from .common import runtime_probe


def build_eval_command_stage_plan(
    *,
    eval_spec: EvalConfigSpec,
    workdir: Path,
    train_stage: StageExecutionPlan,
    validation_cfg: ValidationConfig,
) -> StageExecutionPlan:
    external_runtime = eval_spec.external_runtime
    if external_runtime is None:
        raise PlanningError("eval.command requires eval.external_runtime")
    command_text, command_mode = resolve_external_command_text(
        str(eval_spec.command or ""),
        command_mode=eval_spec.command_mode,
        command_field_name="eval.command",
        mode_field_name="eval.command_mode",
    )
    return StageExecutionPlan(
        name="eval",
        stage_kind=StageKind.EVAL,
        invocation_kind=InvocationKind.EXTERNAL_COMMAND,
        launcher_kind=LauncherKind.EXTERNAL,
        command_text=command_text,
        workdir=workdir,
        topology=ExecutionTopology(
            nodes=external_runtime.nnodes,
            processes_per_node=external_runtime.nproc_per_node,
            master_port=None,
        ),
        allocation=train_stage.allocation,
        estimate=train_stage.estimate,
        capabilities=StageCapabilities(
            ddp_supported=True,
            ddp_required=False,
            uses_gpu=True,
            external_launcher=True,
            runtime_probe=runtime_probe(validation_cfg),
        ),
        python_bin=train_stage.python_bin,
        launcher_cfg=train_stage.launcher_cfg,
        cluster_cfg=train_stage.cluster_cfg,
        script_path=None,
        cli_args={},
        command_mode=command_mode,
        requested_launcher_mode="external",
        max_available_gpus_per_node=train_stage.max_available_gpus_per_node,
    )
