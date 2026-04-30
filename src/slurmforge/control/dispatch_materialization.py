from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..materialization.stage_batch import materialize_stage_batch
from ..planner.stage_batch import compile_stage_batch
from ..resolver.train_eval_pipeline import resolve_stage_inputs_for_train_eval_pipeline
from ..spec import load_experiment_spec_from_snapshot
from ..storage.plan_reader import run_definitions_from_stage_batch
from ..storage.runtime_batches import upsert_runtime_batch
from ..workflow_contract import BATCH_ROLE_DISPATCH, EVAL_STAGE
from .project import project_root_from_pipeline
from ..storage.workflow_state_records import (
    INSTANCE_BLOCKED,
    WorkflowState,
    dequeue_instances,
)


@dataclass(frozen=True)
class MaterializedDispatch:
    batch: object
    selected_instance_ids: tuple[str, ...]


def materialize_eval_dispatch(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    submission_id: str,
    selected_ids: tuple[str, ...],
) -> MaterializedDispatch | None:
    spec = load_experiment_spec_from_snapshot(
        pipeline_root,
        project_root=project_root_from_pipeline(pipeline_root),
    )
    run_defs_by_id = {
        run.run_id: run
        for run in run_definitions_from_stage_batch(plan.stage_batches[EVAL_STAGE])
    }
    runs = tuple(
        run_defs_by_id[state.instances[instance_id].run_id]
        for instance_id in selected_ids
    )
    resolved = resolve_stage_inputs_for_train_eval_pipeline(
        spec,
        plan,
        stage_name=EVAL_STAGE,
        runs=runs,
    )
    selected_ids = _block_unresolved_eval_instances(state, selected_ids, resolved)
    if not selected_ids:
        return None
    root = pipeline_root / "stage_batches" / EVAL_STAGE / "dispatch" / submission_id
    batch = compile_stage_batch(
        spec,
        stage_name=EVAL_STAGE,
        runs=resolved.selected_runs,
        submission_root=root,
        source_ref=f"train_eval_pipeline:{plan.pipeline_id}:{EVAL_STAGE}:{submission_id}",
        input_bindings_by_run=resolved.input_bindings_by_run,
        batch_id=f"{plan.pipeline_id}_{EVAL_STAGE}_{submission_id}",
    )
    if not (root / "manifest.json").exists():
        materialize_stage_batch(
            batch, spec_snapshot=spec.raw, pipeline_root=pipeline_root
        )
    upsert_runtime_batch(
        pipeline_root,
        batch,
        role=BATCH_ROLE_DISPATCH,
        dispatch_id=submission_id,
        source_dispatch_id="",
    )
    return MaterializedDispatch(batch=batch, selected_instance_ids=selected_ids)


def _block_unresolved_eval_instances(
    state: WorkflowState, selected_ids: tuple[str, ...], resolved
) -> tuple[str, ...]:
    if not resolved.blocked_run_ids:
        return selected_ids
    blocked = set(resolved.blocked_run_ids)
    for instance_id in selected_ids:
        instance = state.instances[instance_id]
        if instance.run_id not in blocked:
            continue
        instance.state = INSTANCE_BLOCKED
        instance.reason = resolved.blocked_reasons.get(instance.run_id, "")
    blocked_instance_ids = {
        instance_id
        for instance_id in selected_ids
        if state.instances[instance_id].run_id in blocked
    }
    dequeue_instances(state, blocked_instance_ids)
    return tuple(
        instance_id
        for instance_id in selected_ids
        if instance_id not in blocked_instance_ids
    )
