from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..plans import TRAIN_EVAL_PIPELINE_KIND, RuntimePlan, TrainEvalControllerPlan, TrainEvalPipelinePlan, StageBatchPlan
from ..spec import ExperimentSpec
from ..spec.queries import expand_run_definitions
from .identifiers import train_eval_pipeline_id
from .payloads import control_resources_payload, environment_payload, executor_runtime_payload, notification_payload
from .stage_batch import compile_stage_batch


def compile_train_eval_pipeline_plan(spec: ExperimentSpec, *, submission_root: Path | None = None) -> TrainEvalPipelinePlan:
    runs = expand_run_definitions(spec)
    stage_order = spec.stage_order()
    if stage_order != ("train", "eval"):
        raise ConfigContractError(
            "Train/eval pipeline runs require enabled `stages.train` and `stages.eval` in train -> eval order"
        )
    pipeline_id = train_eval_pipeline_id(spec, runs, stage_order)
    root = (submission_root or spec.storage_root / spec.project / spec.experiment / pipeline_id).resolve()
    stage_batches: dict[str, StageBatchPlan] = {}
    for stage_name in stage_order:
        stage_root = root / "stage_batches" / stage_name
        stage_batches[stage_name] = compile_stage_batch(
            spec,
            stage_name=stage_name,
            runs=runs,
            submission_root=stage_root,
            source_ref=f"train_eval_pipeline:{pipeline_id}",
            batch_id=f"{pipeline_id}_{stage_name}",
        )
    controller = spec.orchestration.controller
    controller_plan = TrainEvalControllerPlan(
        pipeline_id=pipeline_id,
        stage_order=stage_order,
        config_path=str(spec.config_path),
        root_dir=str(root),
        pipeline_kind=TRAIN_EVAL_PIPELINE_KIND,
        resources=control_resources_payload(spec),
        environment_name=controller.environment,
        environment_plan=environment_payload(spec, controller.environment),
        runtime_plan=RuntimePlan(executor=executor_runtime_payload(spec)),
    )
    return TrainEvalPipelinePlan(
        pipeline_id=pipeline_id,
        stage_order=stage_order,
        run_set=tuple(run.run_id for run in runs),
        root_dir=str(root),
        controller_plan=controller_plan,
        stage_batches=stage_batches,
        spec_snapshot_digest=spec.spec_snapshot_digest,
        pipeline_kind=TRAIN_EVAL_PIPELINE_KIND,
        notification_plan=notification_payload(spec),
    )
