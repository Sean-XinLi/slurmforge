from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ..emit.pipeline_gate import write_stage_instance_gate_array_submit_file
from ..io import content_digest
from ..materialization.stage_batch import materialize_stage_batch
from ..planner.stage_batch import compile_stage_batch
from ..resolver.train_eval_pipeline import resolve_stage_inputs_for_train_eval_pipeline
from ..slurm import SlurmClientProtocol, SlurmSubmitOptions
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..spec import load_experiment_spec_from_snapshot
from ..storage.plan_reader import run_definitions_from_stage_batch
from ..storage.runtime_batches import upsert_runtime_batch
from ..workflow_contract import (
    BATCH_ROLE_DISPATCH,
    DISPATCH_CATCHUP_GATE,
    EVAL_STAGE,
    TRAIN_STAGE,
    WORKFLOW_STREAMING,
)
from .instance_reconcile import sync_materialized_statuses
from .project import project_root_from_pipeline
from .stage_submit import ensure_stage_submitted
from .state import record_workflow_event
from .gates import submit_control_gate
from .state_records import (
    DISPATCH_SUBMITTED,
    INSTANCE_BLOCKED,
    INSTANCE_READY,
    INSTANCE_RUNNING,
    INSTANCE_SUBMITTED,
    RELEASE_WINDOWED,
    DispatchSubmissionState,
    WorkflowState,
    dequeue_instances,
    set_submission,
)


def dispatch_ready_instances(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> WorkflowState:
    _dispatch_initial_train(
        pipeline_root,
        plan,
        state,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    _dispatch_ready_eval(
        pipeline_root,
        plan,
        state,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return state


def active_budgeted_gpus(plan, state: WorkflowState) -> int:
    resources_by_instance_id = {
        instance.stage_instance_id: instance.resources
        for batch in plan.stage_batches.values()
        for instance in batch.stage_instances
    }
    return sum(
        resources_by_instance_id[instance.stage_instance_id].total_gpus
        for instance in state.instances.values()
        if instance.state in {INSTANCE_SUBMITTED, INSTANCE_RUNNING}
        and instance.stage_instance_id in resources_by_instance_id
    )


def _dispatch_initial_train(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    if _has_stage_submission(state, TRAIN_STAGE):
        return
    ready_train_ids = tuple(
        instance_id
        for instance_id in state.dispatch_queue
        if state.instances[instance_id].stage_name == TRAIN_STAGE
        and state.instances[instance_id].state == INSTANCE_READY
    )
    if not ready_train_ids:
        return
    batch = plan.stage_batches[TRAIN_STAGE]
    submission_id = f"{TRAIN_STAGE}_initial"
    group_job_ids = ensure_stage_submitted(
        pipeline_root,
        batch,
        client=client,
        state_group_id=submission_id,
    )
    _record_submission(
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        budgeted_gpus=_batch_budgeted_gpus(batch),
    )
    _submit_dispatch_gates(
        pipeline_root,
        plan,
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    dequeue_instances(state, set(ready_train_ids))
    state.state = WORKFLOW_STREAMING
    state.current_stage = TRAIN_STAGE
    record_workflow_event(
        pipeline_root,
        "dispatch_submitted",
        submission_id=submission_id,
        stage=TRAIN_STAGE,
        instance_ids=list(ready_train_ids),
    )


def _dispatch_ready_eval(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    ready_ids = tuple(
        instance_id
        for instance_id in state.dispatch_queue
        if state.instances[instance_id].stage_name == EVAL_STAGE
        and state.instances[instance_id].state == INSTANCE_READY
    )
    if not ready_ids:
        return
    if state.release_policy == RELEASE_WINDOWED and not _window_ready(
        plan, state, ready_ids
    ):
        return
    selected_ids = _select_with_gpu_budget(plan, state, ready_ids)
    if not selected_ids:
        return
    spec = load_experiment_spec_from_snapshot(
        pipeline_root,
        project_root=project_root_from_pipeline(pipeline_root),
    )
    run_defs_by_id = {
        run.run_id: run
        for run in run_definitions_from_stage_batch(plan.stage_batches[EVAL_STAGE])
    }
    runs = tuple(run_defs_by_id[state.instances[instance_id].run_id] for instance_id in selected_ids)
    resolved = resolve_stage_inputs_for_train_eval_pipeline(
        spec,
        plan,
        stage_name=EVAL_STAGE,
        runs=runs,
    )
    if resolved.blocked_run_ids:
        blocked = set(resolved.blocked_run_ids)
        for instance_id in selected_ids:
            instance = state.instances[instance_id]
            if instance.run_id not in blocked:
                continue
            instance.state = INSTANCE_BLOCKED
            instance.reason = resolved.blocked_reasons.get(instance.run_id, "")
        selected_ids = tuple(
            instance_id
            for instance_id in selected_ids
            if state.instances[instance_id].run_id not in blocked
        )
        dequeue_instances(
            state,
            {
                instance_id
                for instance_id in ready_ids
                if state.instances[instance_id].run_id in blocked
            },
        )
    if not selected_ids:
        return

    submission_id = _submission_id(EVAL_STAGE, selected_ids)
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
        materialize_stage_batch(batch, spec_snapshot=spec.raw, pipeline_root=pipeline_root)
    upsert_runtime_batch(
        pipeline_root,
        batch,
        role=BATCH_ROLE_DISPATCH,
        shard_id=submission_id,
        source_train_group_id="",
    )
    group_job_ids = ensure_stage_submitted(
        pipeline_root,
        batch,
        client=client,
        state_group_id=submission_id,
    )
    _record_submission(
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        budgeted_gpus=_batch_budgeted_gpus(batch),
    )
    _submit_dispatch_gates(
        pipeline_root,
        plan,
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    dequeue_instances(state, set(selected_ids))
    state.current_stage = EVAL_STAGE
    sync_materialized_statuses(state, batch)
    record_workflow_event(
        pipeline_root,
        "dispatch_submitted",
        submission_id=submission_id,
        stage=EVAL_STAGE,
        instance_ids=list(selected_ids),
    )


def _select_with_gpu_budget(plan, state: WorkflowState, ready_ids: tuple[str, ...]) -> tuple[str, ...]:
    max_gpus = plan.stage_batches[EVAL_STAGE].budget_plan.max_available_gpus
    if max_gpus <= 0:
        return ready_ids
    active = active_budgeted_gpus(plan, state)
    selected: list[str] = []
    selected_gpus = 0
    instances_by_id = {
        instance.stage_instance_id: instance
        for instance in plan.stage_batches[EVAL_STAGE].stage_instances
    }
    for instance_id in ready_ids:
        plan_instance = instances_by_id[instance_id]
        required = plan_instance.resources.total_gpus
        if required <= 0:
            selected.append(instance_id)
            continue
        if active + selected_gpus + required > max_gpus:
            continue
        selected.append(instance_id)
        selected_gpus += required
    return tuple(selected)


def _window_ready(plan, state: WorkflowState, ready_ids: tuple[str, ...]) -> bool:
    window_size = int(getattr(plan, "dispatch_window_size", 1) or 1)
    window_seconds = int(getattr(plan, "dispatch_window_seconds", 0) or 0)
    if len(ready_ids) >= window_size or window_seconds <= 0:
        return True
    now = datetime.now(timezone.utc)
    for instance_id in ready_ids:
        ready_at = state.instances[instance_id].ready_at
        if not ready_at:
            continue
        if (now - datetime.fromisoformat(ready_at)).total_seconds() >= window_seconds:
            return True
    return False


def _record_submission(
    state: WorkflowState,
    batch,
    *,
    submission_id: str,
    group_job_ids: dict[str, str],
    budgeted_gpus: int,
) -> None:
    ordered_job_ids = tuple(
        group_job_ids.get(group.group_id, "")
        for group in batch.group_plans
    )
    submission = DispatchSubmissionState(
        submission_id=submission_id,
        stage_name=batch.stage_name,
        instance_ids=tuple(instance.stage_instance_id for instance in batch.stage_instances),
        root=str(Path(batch.submission_root).resolve()),
        scheduler_job_ids=ordered_job_ids,
        budgeted_gpus=budgeted_gpus,
        state=DISPATCH_SUBMITTED,
    )
    set_submission(state, submission)
    jobs_by_instance: dict[str, tuple[str, str]] = {}
    for group_index, group in enumerate(batch.group_plans):
        job_id = ordered_job_ids[group_index]
        for task_index, instance_id in enumerate(group.stage_instance_ids):
            jobs_by_instance[instance_id] = (
                job_id,
                str(task_index) if group.array_size > 1 else "",
            )
    for instance_id in submission.instance_ids:
        instance = state.instances[instance_id]
        instance.submission_id = submission_id
        instance.state = INSTANCE_SUBMITTED
        job_id, task_id = jobs_by_instance.get(instance_id, ("", ""))
        instance.scheduler_job_id = job_id
        instance.scheduler_array_task_id = task_id


def _submit_dispatch_gates(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    batch,
    *,
    submission_id: str,
    group_job_ids: dict[str, str],
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    for group in batch.group_plans:
        scheduler_job_id = group_job_ids.get(group.group_id)
        if not scheduler_job_id:
            continue
        gate_path = write_stage_instance_gate_array_submit_file(
            plan,
            submission_id=submission_id,
            group_id=group.group_id,
            stage_instance_ids=tuple(group.stage_instance_ids),
        )
        gate_job_id = client.submit(
            gate_path,
            options=SlurmSubmitOptions(dependency=f"aftercorr:{scheduler_job_id}"),
        )
        record_workflow_event(
            pipeline_root,
            "stage_instance_gate_submitted",
            submission_id=submission_id,
            stage=batch.stage_name,
            group_id=group.group_id,
            scheduler_job_id=gate_job_id,
            dependency_job_id=scheduler_job_id,
        )
    if group_job_ids:
        submit_control_gate(
            pipeline_root,
            state,
            plan,
            DISPATCH_CATCHUP_GATE,
            group_id=submission_id,
            dependency_job_ids=tuple(group_job_ids.values()),
            client=client,
            max_dependency_length=max_dependency_length,
        )


def _has_stage_submission(state: WorkflowState, stage_name: str) -> bool:
    return any(
        submission.stage_name == stage_name for submission in state.submissions.values()
    )


def _submission_id(stage_name: str, instance_ids: tuple[str, ...]) -> str:
    digest = content_digest({"stage": stage_name, "instances": sorted(instance_ids)}, prefix=12)
    return f"{stage_name}_{digest}"


def _batch_budgeted_gpus(batch) -> int:
    if batch.budget_plan.max_available_gpus <= 0:
        return 0
    total = 0
    for group in batch.group_plans:
        throttle = group.array_throttle or group.array_size
        total += group.gpus_per_task * throttle
    return total
