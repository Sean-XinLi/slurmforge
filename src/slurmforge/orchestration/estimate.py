from __future__ import annotations

from ..planner.resource_estimate import (
    build_resource_estimate as _build_resource_estimate,
)
from ..planner.resource_estimate import (
    render_resource_estimate as _render_resource_estimate,
)
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan


def build_resource_estimate_for_plan(
    plan: StageBatchPlan | TrainEvalPipelinePlan,
):
    return _build_resource_estimate(plan)


def render_resource_estimate_for_plan(
    plan: StageBatchPlan | TrainEvalPipelinePlan,
) -> list[str]:
    return _render_resource_estimate(_build_resource_estimate(plan))
