from __future__ import annotations

from typing import Any

from ..train_eval import (
    TRAIN_EVAL_PIPELINE_KIND,
    TrainEvalControllerPlan,
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
    controller = dict(payload["controller_plan"])
    require_plan_schema(controller, name="train_eval_controller_plan")
    pipeline_kind = str(payload["pipeline_kind"])
    controller_pipeline_kind = str(controller["pipeline_kind"])
    if pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_pipeline_plan.pipeline_kind is not supported: {pipeline_kind}"
        )
    if controller_pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_controller_plan.pipeline_kind is not supported: {controller_pipeline_kind}"
        )
    return TrainEvalPipelinePlan(
        pipeline_id=str(payload["pipeline_id"]),
        stage_order=tuple(str(item) for item in payload["stage_order"]),
        run_set=tuple(str(item) for item in payload["run_set"]),
        root_dir=str(payload["root_dir"]),
        controller_plan=TrainEvalControllerPlan(
            pipeline_id=str(controller["pipeline_id"]),
            stage_order=tuple(str(item) for item in controller["stage_order"]),
            config_path=str(controller["config_path"]),
            root_dir=str(controller["root_dir"]),
            pipeline_kind=controller_pipeline_kind,
            resources=control_resources_plan_from_dict(controller["resources"]),
            environment_name=str(controller["environment_name"]),
            environment_plan=environment_plan_from_dict(controller["environment_plan"]),
            runtime_plan=runtime_plan_from_dict(controller["runtime_plan"]),
        ),
        stage_batches={
            str(name): stage_batch_plan_from_dict(batch)
            for name, batch in dict(payload["stage_batches"]).items()
        },
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        pipeline_kind=pipeline_kind,
        notification_plan=notification_plan_from_dict(payload["notification_plan"]),
    )
