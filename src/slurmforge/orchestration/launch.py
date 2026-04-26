from __future__ import annotations

from typing import Literal

from ..emit import write_controller_submit_file
from ..plans import SourcedStageBatchPlan
from ..spec import ExperimentSpec
from ..storage import materialize_sourced_stage_batch_plan, write_pipeline_layout, write_stage_batch_layout
from ..submission import PreparedSubmission, prepare_stage_submission, submit_prepared_stage_batch
from .controller import submit_controller_job

ExecutionMode = Literal["preview", "emit", "submit"]


def _materialize_stage_batch(spec: ExperimentSpec, batch) -> PreparedSubmission:
    write_stage_batch_layout(batch, spec_snapshot=spec.raw)
    return prepare_stage_submission(batch)


def _materialize_sourced_stage_batch(plan: SourcedStageBatchPlan) -> SourcedStageBatchPlan:
    return materialize_sourced_stage_batch_plan(plan)


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
    return concrete, _submit_materialized_stage_batch(prepared)


def emit_stage_batch(spec: ExperimentSpec, batch, *, submit: bool) -> None:
    prepared = _materialize_stage_batch(spec, batch)
    if not submit:
        print(f"[OK] emitted stage batch: {batch.submission_root}")
        return
    group_job_ids = _submit_materialized_stage_batch(prepared)
    print(f"[OK] submitted stage batch: {batch.submission_root}")
    print(f"[OK] scheduler_job_ids={','.join(group_job_ids.values())}")


def execute_stage_batch_plan(spec: ExperimentSpec, batch, *, mode: ExecutionMode) -> None:
    if mode == "preview":
        return
    emit_stage_batch(spec, batch, submit=mode == "submit")


def emit_pipeline(spec: ExperimentSpec, plan, *, submit: bool) -> None:
    write_pipeline_layout(plan, spec_snapshot=spec.raw)
    if "train" in plan.stage_batches:
        prepare_stage_submission(plan.stage_batches["train"])
    if not submit:
        write_controller_submit_file(plan)
        print(f"[OK] emitted pipeline: {plan.root_dir}")
        return
    record = submit_controller_job(plan)
    print(f"[OK] submitted pipeline controller: {record.scheduler_job_id}")
    print(f"[OK] pipeline_root={plan.root_dir}")


def execute_pipeline_plan(spec: ExperimentSpec, plan, *, mode: ExecutionMode) -> None:
    if mode == "preview":
        return
    emit_pipeline(spec, plan, submit=mode == "submit")
