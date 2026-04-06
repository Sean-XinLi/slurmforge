from __future__ import annotations

from pathlib import Path

from ...config.api import EvalConfigSpec
from ...config.runtime import ValidationConfig
from ...config.utils import resolve_path
from ..contracts import StageExecutionPlan
from ..validator import validate_stage_execution_plan
from .command import build_eval_command_stage_plan
from .script import build_eval_script_stage_plan


def build_eval_stage_plan(
    *,
    project_root: Path,
    eval_spec: EvalConfigSpec,
    default_workdir: Path,
    train_stage: StageExecutionPlan,
    run_idx: int,
    validation_cfg: ValidationConfig,
    run_args: dict[str, object],
    model_overrides: dict[str, object],
) -> StageExecutionPlan | None:
    if not eval_spec.enabled:
        return None

    workdir = resolve_path(project_root, eval_spec.workdir, str(default_workdir))

    if eval_spec.command:
        return validate_stage_execution_plan(
            build_eval_command_stage_plan(
                eval_spec=eval_spec,
                workdir=workdir,
                train_stage=train_stage,
                validation_cfg=validation_cfg,
            ),
            validation_cfg,
        )

    return validate_stage_execution_plan(
        build_eval_script_stage_plan(
            project_root=project_root,
            eval_spec=eval_spec,
            workdir=workdir,
            train_stage=train_stage,
            run_idx=run_idx,
            validation_cfg=validation_cfg,
            run_args=run_args,
            model_overrides=model_overrides,
        ),
        validation_cfg,
    )
