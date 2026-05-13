from __future__ import annotations

from ..control.initial_prepare import prepare_initial_pipeline_files
from ..control.workflow import submit_initial_pipeline
from ..materialization.sourced import materialize_sourced_stage_batch
from ..materialization.stage_batch import materialize_stage_batch
from ..materialization.train_eval import materialize_train_eval_pipeline
from ..plans.sources import SourcedStageBatchPlan
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..spec import ExperimentSpec
from ..submission.notifications import submit_stage_batch_notification
from ..submission.generation import prepare_stage_submission
from ..submission.models import PreparedSubmission
from ..submission.submitter import submit_prepared_stage_batch
from .results import (
    ExecutionMode,
    StageBatchExecutionResult,
    SourcedStageBatchExecutionResult,
    TrainEvalPipelineExecutionResult,
)


def _materialize_stage_batch(
    spec: ExperimentSpec, batch: StageBatchPlan
) -> PreparedSubmission:
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
    notification = submit_stage_batch_notification(concrete.batch, group_job_ids)
    return SourcedStageBatchExecutionResult(
        plan=concrete,
        root=concrete.batch.submission_root,
        mode="submit",
        submitted=True,
        scheduler_job_ids=group_job_ids,
        notification_job_ids=()
        if notification is None
        else notification.scheduler_job_ids,
    )


def emit_stage_batch(
    spec: ExperimentSpec, batch: StageBatchPlan, *, submit: bool
) -> StageBatchExecutionResult:
    prepared = _materialize_stage_batch(spec, batch)
    if not submit:
        return StageBatchExecutionResult(root=batch.submission_root, mode="emit")
    group_job_ids = _submit_materialized_stage_batch(prepared)
    notification = submit_stage_batch_notification(batch, group_job_ids)
    return StageBatchExecutionResult(
        root=batch.submission_root,
        mode="submit",
        submitted=True,
        scheduler_job_ids=group_job_ids,
        notification_job_ids=()
        if notification is None
        else notification.scheduler_job_ids,
    )


def execute_stage_batch_plan(
    spec: ExperimentSpec, batch: StageBatchPlan, *, mode: ExecutionMode
) -> StageBatchExecutionResult:
    if mode == "preview":
        return StageBatchExecutionResult(root=batch.submission_root, mode="preview")
    return emit_stage_batch(spec, batch, submit=mode == "submit")


def emit_train_eval_pipeline(
    spec: ExperimentSpec, plan: TrainEvalPipelinePlan, *, submit: bool
) -> TrainEvalPipelineExecutionResult:
    materialize_train_eval_pipeline(plan, spec_snapshot=spec.raw)
    if not submit:
        prepare_initial_pipeline_files(plan)
        return TrainEvalPipelineExecutionResult(root=plan.root_dir, mode="emit")
    result = submit_initial_pipeline(plan)
    return TrainEvalPipelineExecutionResult(
        root=plan.root_dir,
        mode="submit",
        submitted=True,
        stage_job_ids=result.submitted_stage_job_ids,
        control_job_ids=result.submitted_control_job_ids,
    )


def execute_train_eval_pipeline_plan(
    spec: ExperimentSpec, plan: TrainEvalPipelinePlan, *, mode: ExecutionMode
) -> TrainEvalPipelineExecutionResult:
    if mode == "preview":
        return TrainEvalPipelineExecutionResult(root=plan.root_dir, mode="preview")
    return emit_train_eval_pipeline(spec, plan, submit=mode == "submit")
