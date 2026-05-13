from __future__ import annotations

from typing import Any

from ...errors import RecordContractError
from ...record_fields import (
    required_int,
    required_object,
    required_record,
    required_string,
    required_string_tuple,
)
from ...release_policy_contract import RELEASE_POLICIES
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
    control = required_object(
        payload, "control_plan", label="train_eval_pipeline_plan"
    )
    require_plan_schema(control, name="train_eval_control_plan")
    pipeline_kind = required_string(
        payload, "pipeline_kind", label="train_eval_pipeline_plan", non_empty=True
    )
    control_pipeline_kind = required_string(
        control, "pipeline_kind", label="train_eval_control_plan", non_empty=True
    )
    if pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_pipeline_plan.pipeline_kind is not supported: {pipeline_kind}"
        )
    if control_pipeline_kind != TRAIN_EVAL_PIPELINE_KIND:
        raise ValueError(
            f"train_eval_control_plan.pipeline_kind is not supported: {control_pipeline_kind}"
        )
    return TrainEvalPipelinePlan(
        pipeline_id=required_string(
            payload, "pipeline_id", label="train_eval_pipeline_plan", non_empty=True
        ),
        stage_order=required_string_tuple(
            payload, "stage_order", label="train_eval_pipeline_plan"
        ),
        run_set=required_string_tuple(
            payload, "run_set", label="train_eval_pipeline_plan"
        ),
        root_dir=required_string(
            payload, "root_dir", label="train_eval_pipeline_plan", non_empty=True
        ),
        control_plan=TrainEvalControlPlan(
            pipeline_id=required_string(
                control, "pipeline_id", label="train_eval_control_plan", non_empty=True
            ),
            stage_order=required_string_tuple(
                control, "stage_order", label="train_eval_control_plan"
            ),
            config_path=required_string(
                control, "config_path", label="train_eval_control_plan", non_empty=True
            ),
            root_dir=required_string(
                control, "root_dir", label="train_eval_control_plan", non_empty=True
            ),
            pipeline_kind=control_pipeline_kind,
            resources=control_resources_plan_from_dict(
                required_object(control, "resources", label="train_eval_control_plan")
            ),
            environment_name=required_string(
                control, "environment_name", label="train_eval_control_plan"
            ),
            environment_plan=environment_plan_from_dict(
                required_object(
                    control, "environment_plan", label="train_eval_control_plan"
                )
            ),
            runtime_plan=runtime_plan_from_dict(
                required_object(control, "runtime_plan", label="train_eval_control_plan")
            ),
        ),
        stage_batches={
            str(name): stage_batch_plan_from_dict(
                required_record(batch, "train_eval_pipeline_plan.stage_batches item")
            )
            for name, batch in required_object(
                payload, "stage_batches", label="train_eval_pipeline_plan"
            ).items()
        },
        spec_snapshot_digest=required_string(
            payload,
            "spec_snapshot_digest",
            label="train_eval_pipeline_plan",
            non_empty=True,
        ),
        pipeline_kind=pipeline_kind,
        notification_plan=notification_plan_from_dict(
            required_object(
                payload, "notification_plan", label="train_eval_pipeline_plan"
            )
        ),
        release_policy=_release_policy(payload),
        dispatch_window_size=required_int(
            payload, "dispatch_window_size", label="train_eval_pipeline_plan"
        ),
        dispatch_window_seconds=required_int(
            payload, "dispatch_window_seconds", label="train_eval_pipeline_plan"
        ),
    )


def _release_policy(payload: dict[str, Any]) -> str:
    value = required_string(
        payload, "release_policy", label="train_eval_pipeline_plan", non_empty=True
    )
    if value not in RELEASE_POLICIES:
        raise RecordContractError(f"Unsupported train/eval release_policy: {value}")
    return value
