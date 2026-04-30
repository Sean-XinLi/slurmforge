from __future__ import annotations

from typing import Any

from ...workflow_contract import TRAIN_EVAL_PIPELINE_KIND
from ..train_eval import (
    TrainEvalControlPlan,
    TrainEvalPipelinePlan,
)
from .common import require_plan_schema
from .notifications import notification_plan_from_dict
from .resources import control_resources_plan_from_dict
from .runtime import environment_plan_from_dict, runtime_plan_from_dict
from .stage import stage_batch_plan_from_dict


def train_eval_pipeline_plan_from_dict(
    payload: dict[str, Any],
) -> TrainEvalPipelinePlan:
    require_plan_schema(payload, name="train_eval_pipeline_plan")
    control = dict(payload["control_plan"])
    require_plan_schema(control, name="train_eval_control_plan")
    pipeline_kind = str(payload["pipeline_kind"])
    control_pipeline_kind = str(control["pipeline_kind"])
    if pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_pipeline_plan.pipeline_kind is not supported: {pipeline_kind}"
        )
    if control_pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_control_plan.pipeline_kind is not supported: {control_pipeline_kind}"
        )
    return TrainEvalPipelinePlan(
        pipeline_id=str(payload["pipeline_id"]),
        stage_order=tuple(str(item) for item in payload["stage_order"]),
        run_set=tuple(str(item) for item in payload["run_set"]),
        root_dir=str(payload["root_dir"]),
        control_plan=TrainEvalControlPlan(
            pipeline_id=str(control["pipeline_id"]),
            stage_order=tuple(str(item) for item in control["stage_order"]),
            config_path=str(control["config_path"]),
            root_dir=str(control["root_dir"]),
            pipeline_kind=control_pipeline_kind,
            resources=control_resources_plan_from_dict(control["resources"]),
            environment_name=str(control["environment_name"]),
            environment_plan=environment_plan_from_dict(control["environment_plan"]),
            runtime_plan=runtime_plan_from_dict(control["runtime_plan"]),
        ),
        stage_batches={
            str(name): stage_batch_plan_from_dict(batch)
            for name, batch in dict(payload["stage_batches"]).items()
        },
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        pipeline_kind=pipeline_kind,
        notification_plan=notification_plan_from_dict(payload["notification_plan"]),
    )
