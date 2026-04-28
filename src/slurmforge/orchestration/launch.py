from __future__ import annotations

from ..emit.controller import write_controller_submit_file
from ..materialization.sourced import materialize_sourced_stage_batch
from ..materialization.stage_batch import materialize_stage_batch
from ..materialization.train_eval import materialize_train_eval_pipeline
from ..plans.sources import SourcedStageBatchPlan
from ..spec import ExperimentSpec
from ..submission.finalizer import submit_stage_batch_finalizer
from ..submission.generation import prepare_stage_submission
from ..submission.models import PreparedSubmission
from ..submission.submitter import submit_prepared_stage_batch
from .controller import submit_controller_job
from .results import (
    ExecutionMode,
    StageBatchExecutionResult,
    SourcedStageBatchExecutionResult,
    TrainEvalPipelineExecutionResult,
)


def _materialize_stage_batch(spec: ExperimentSpec, batch) -> PreparedSubmission:
    materialize_stage_batch(batch, spec_snapshot=spec.raw)
    return prepare_stage_submission(batch)


def _materialize_sourced_stage_batch(
    plan: SourcedStageBatchPlan,
) -> SourcedStageBatchPlan:
    return materialize_sourced_stage_batch(plan)


def _submit_materialized_stage_batch(prepared: PreparedSubmission) -> dict[str, str]:
    return submit_prepared_stage_batch(prepared)


def emit_sourced_stage_batch(
    plan: SourcedStageBatchPlan,
    *,
    submit: bool,
) -> SourcedStageBatchExecutionResult:
    concrete = _materialize_sourced_stage_batch(plan)
    prepared = prepare_stage_submission(concrete.batch)
    if not submit:
        return SourcedStageBatchExecutionResult(
            plan=concrete,
            root=concrete.batch.submission_root,
            mode="emit",
        )
    group_job_ids = _submit_materialized_stage_batch(prepared)
    notification = submit_stage_batch_finalizer(concrete.batch, group_job_ids)
    return SourcedStageBatchExecutionResult(
        plan=concrete,
        root=concrete.batch.submission_root,
        mode="submit",
        submitted=True,
        scheduler_job_ids=group_job_ids,
        notification_job_id=None
        if notification is None
        else notification.scheduler_job_id,
    )


def emit_stage_batch(
    spec: ExperimentSpec, batch, *, submit: bool
) -> StageBatchExecutionResult:
    prepared = _materialize_stage_batch(spec, batch)
    if not submit:
        return StageBatchExecutionResult(root=batch.submission_root, mode="emit")
    group_job_ids = _submit_materialized_stage_batch(prepared)
    notification = submit_stage_batch_finalizer(batch, group_job_ids)
    return StageBatchExecutionResult(
        root=batch.submission_root,
        mode="submit",
        submitted=True,
        scheduler_job_ids=group_job_ids,
        notification_job_id=None
        if notification is None
        else notification.scheduler_job_id,
    )


def execute_stage_batch_plan(
    spec: ExperimentSpec, batch, *, mode: ExecutionMode
) -> StageBatchExecutionResult:
    if mode == "preview":
        return StageBatchExecutionResult(root=batch.submission_root, mode="preview")
    return emit_stage_batch(spec, batch, submit=mode == "submit")


def emit_train_eval_pipeline(
    spec: ExperimentSpec, plan, *, submit: bool
) -> TrainEvalPipelineExecutionResult:
    materialize_train_eval_pipeline(plan, spec_snapshot=spec.raw)
    if "train" in plan.stage_batches:
        prepare_stage_submission(plan.stage_batches["train"])
    if not submit:
        write_controller_submit_file(plan)
        return TrainEvalPipelineExecutionResult(root=plan.root_dir, mode="emit")
    record = submit_controller_job(plan)
    return TrainEvalPipelineExecutionResult(
        root=plan.root_dir,
        mode="submit",
        submitted=True,
        controller_job_id=record.scheduler_job_id,
    )


def execute_train_eval_pipeline_plan(
    spec: ExperimentSpec, plan, *, mode: ExecutionMode
) -> TrainEvalPipelineExecutionResult:
    if mode == "preview":
        return TrainEvalPipelineExecutionResult(root=plan.root_dir, mode="preview")
    return emit_train_eval_pipeline(spec, plan, submit=mode == "submit")
