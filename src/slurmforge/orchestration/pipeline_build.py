from __future__ import annotations

from ..planner.summaries import (
    summarize_train_eval_pipeline_plan as _summarize_pipeline_plan,
)
from ..planner.train_eval_pipeline import compile_train_eval_pipeline_plan
from ..spec import ExperimentSpec


def build_train_eval_pipeline_plan(spec: ExperimentSpec):
    return compile_train_eval_pipeline_plan(spec)


def summarize_train_eval_pipeline_plan(plan) -> list[str]:
    return _summarize_pipeline_plan(plan)
