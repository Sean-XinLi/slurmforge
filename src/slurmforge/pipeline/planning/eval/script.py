from __future__ import annotations

from pathlib import Path

from ....errors import PlanningError
from ...config.api import EvalConfigSpec
from ...config.runtime import ValidationConfig
from ..contracts import ExecutionTopology, StageCapabilities, StageExecutionPlan
from ..enums import InvocationKind, LauncherKind, StageKind
from ...launch import build_stage_command
from .common import runtime_probe
from .launcher_merge import resolve_eval_launcher


def build_eval_script_stage_plan(
    *,
    project_root: Path,
    eval_spec: EvalConfigSpec,
    workdir: Path,
    train_stage: StageExecutionPlan,
    run_idx: int,
    validation_cfg: ValidationConfig,
    run_args: dict[str, object],
    model_overrides: dict[str, object],
) -> StageExecutionPlan:
    if not eval_spec.script:
        raise PlanningError("eval.enabled=true requires either eval.command or eval.script")

    script_path = Path(eval_spec.script)
    if not script_path.is_absolute():
        script_path = (project_root / script_path).resolve()

    eval_args = dict(eval_spec.args)
    if eval_spec.pass_run_args:
        eval_args[eval_spec.run_args_flag] = run_args
    if eval_spec.pass_model_overrides:
        eval_args[eval_spec.model_overrides_flag] = model_overrides

    eval_launcher, requested_mode, launcher_kind = resolve_eval_launcher(
        eval_spec=eval_spec,
        train_stage=train_stage,
    )
    command_text, runtime = build_stage_command(
        script_path=script_path,
        args=eval_args,
        launcher_cfg=eval_launcher,
        launch_mode="ddp" if launcher_kind == LauncherKind.DDP else "single",
        run_idx=run_idx,
    )
    return StageExecutionPlan(
        name="eval",
        stage_kind=StageKind.EVAL,
        invocation_kind=InvocationKind.EVAL_SCRIPT,
        launcher_kind=launcher_kind,
        command_text=command_text,
        workdir=workdir,
        topology=ExecutionTopology(
            nodes=runtime.nnodes,
            processes_per_node=runtime.nproc_per_node,
            master_port=runtime.master_port,
        ),
        allocation=train_stage.allocation,
        estimate=train_stage.estimate,
        capabilities=StageCapabilities(
            ddp_supported=True,
            ddp_required=False,
            uses_gpu=True,
            external_launcher=False,
            runtime_probe=runtime_probe(validation_cfg),
        ),
        python_bin=eval_launcher.python_bin,
        launcher_cfg=eval_launcher,
        cluster_cfg=train_stage.cluster_cfg,
        script_path=script_path,
        cli_args=eval_args,
        command_mode=None,
        requested_launcher_mode=requested_mode,
        max_gpus_per_job=train_stage.max_gpus_per_job,
    )
