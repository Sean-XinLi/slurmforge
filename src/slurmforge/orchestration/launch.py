from __future__ import annotations

from typing import Literal

from ..emit import write_controller_submit_file
from ..plans import SourcedStageBatchPlan
from ..read_models.status import refresh_stage_batch_status, refresh_train_eval_pipeline_status
from ..spec import ExperimentSpec
from ..storage.layout import write_stage_batch_layout, write_train_eval_pipeline_layout
from ..storage.materialization import materialize_sourced_stage_batch_plan
from ..submission import PreparedSubmission, prepare_stage_submission, submit_prepared_stage_batch, submit_stage_batch_finalizer
from .controller import submit_controller_job

ExecutionMode = Literal["preview", "emit", "submit"]


def _materialize_stage_batch(spec: ExperimentSpec, batch) -> PreparedSubmission:
    write_stage_batch_layout(batch, spec_snapshot=spec.raw)
    refresh_stage_batch_status(batch.submission_root)
    return prepare_stage_submission(batch)


def _materialize_sourced_stage_batch(plan: SourcedStageBatchPlan) -> SourcedStageBatchPlan:
    concrete = materialize_sourced_stage_batch_plan(plan)
    refresh_stage_batch_status(concrete.batch.submission_root)
    return concrete


def _submit_materialized_stage_batch(prepared: PreparedSubmission) -> dict[str, str]:
    return submit_prepared_stage_batch(prepared)


def emit_sourced_stage_batch(
    plan: SourcedStageBatchPlan,
    *,
    submit: bool,
) -> tuple[SourcedStageBatchPlan, dict[str, str] | None]:
    concrete = _materialize_sourced_stage_batch(plan)
    prepared = prepare_stage_submission(concrete.batch)
    if not submit:
        return concrete, None
    group_job_ids = _submit_materialized_stage_batch(prepared)
    submit_stage_batch_finalizer(concrete.batch, group_job_ids)
    return concrete, group_job_ids


def emit_stage_batch(spec: ExperimentSpec, batch, *, submit: bool) -> None:
    prepared = _materialize_stage_batch(spec, batch)
    if not submit:
        print(f"[OK] emitted stage batch: {batch.submission_root}")
        return
    group_job_ids = _submit_materialized_stage_batch(prepared)
    submit_stage_batch_finalizer(batch, group_job_ids)
    print(f"[OK] submitted stage batch: {batch.submission_root}")
    print(f"[OK] scheduler_job_ids={','.join(group_job_ids.values())}")


def execute_stage_batch_plan(spec: ExperimentSpec, batch, *, mode: ExecutionMode) -> None:
    if mode == "preview":
        return
    emit_stage_batch(spec, batch, submit=mode == "submit")


def emit_train_eval_pipeline(spec: ExperimentSpec, plan, *, submit: bool) -> None:
    write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
    refresh_train_eval_pipeline_status(plan.root_dir)
    if "train" in plan.stage_batches:
        prepare_stage_submission(plan.stage_batches["train"])
    if not submit:
        write_controller_submit_file(plan)
        print(f"[OK] emitted train/eval pipeline: {plan.root_dir}")
        return
    record = submit_controller_job(plan)
    print(f"[OK] submitted train/eval pipeline controller: {record.scheduler_job_id}")
    print(f"[OK] pipeline_root={plan.root_dir}")


def execute_train_eval_pipeline_plan(spec: ExperimentSpec, plan, *, mode: ExecutionMode) -> None:
    if mode == "preview":
        return
    emit_train_eval_pipeline(spec, plan, submit=mode == "submit")
