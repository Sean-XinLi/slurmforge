from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...config.api import ExperimentSpec
from ..contracts import StageExecutionPlan
from ..eval import build_eval_stage_plan
from ..train import build_train_resolution, prepare_train_plan
from ..train.context import PreparedTrainPlan


@dataclass(frozen=True)
class BuiltRunStages:
    prepared: PreparedTrainPlan
    train_stage: StageExecutionPlan
    eval_stage: StageExecutionPlan | None


def build_run_stages(
    spec: ExperimentSpec,
    *,
    project_root: Path,
    batch_root: Path,
    run_index: int,
) -> BuiltRunStages:
    prepared = prepare_train_plan(
        spec,
        project_root=project_root,
        batch_root=batch_root,
    )
    train_stage = build_train_resolution(prepared, run_index=run_index)
    eval_stage = build_eval_stage_plan(
        project_root=project_root,
        eval_spec=prepared.eval_spec,
        default_workdir=train_stage.workdir,
        train_stage=train_stage,
        run_idx=run_index - 1,
        validation_cfg=prepared.validation_cfg,
        run_args=prepared.run_args,
        model_overrides=prepared.model_overrides,
    )
    return BuiltRunStages(
        prepared=prepared,
        train_stage=train_stage,
        eval_stage=eval_stage,
    )
