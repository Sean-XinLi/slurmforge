from __future__ import annotations

from ..planner.summaries import (
    summarize_train_eval_pipeline_plan as _summarize_pipeline_plan,
)
from ..planner.train_eval_pipeline import compile_train_eval_pipeline_plan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..spec import ExperimentSpec


def build_train_eval_pipeline_plan(spec: ExperimentSpec) -> TrainEvalPipelinePlan:
    return compile_train_eval_pipeline_plan(spec)


def summarize_train_eval_pipeline_plan(plan: TrainEvalPipelinePlan) -> list[str]:
    return _summarize_pipeline_plan(plan)
