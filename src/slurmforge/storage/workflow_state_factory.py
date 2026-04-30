from __future__ import annotations

from ..workflow_contract import (
    EVAL_STAGE,
    TRAIN_EVAL_PIPELINE_KIND,
    TRAIN_STAGE,
    WORKFLOW_PLANNED,
)
from ..plans.train_eval import TrainEvalPipelinePlan
from .workflow_state_records import (
    DEPENDENCY_WAITING,
    INSTANCE_PLANNED,
    INSTANCE_READY,
    DependencyState,
    StageInstanceState,
    WorkflowState,
    dependency_key,
)


def build_initial_workflow_state(plan: TrainEvalPipelinePlan) -> WorkflowState:
    instances: dict[str, StageInstanceState] = {}
    dispatch_queue: list[str] = []
    for stage_name in plan.stage_order:
        batch = plan.stage_batches[stage_name]
        for stage_instance in batch.stage_instances:
            initial_state = INSTANCE_READY if stage_name == TRAIN_STAGE else INSTANCE_PLANNED
            instance = StageInstanceState(
                stage_instance_id=stage_instance.stage_instance_id,
                stage_name=stage_name,
                run_id=stage_instance.run_id,
                state=initial_state,
                submission_id="",
                scheduler_job_id="",
                scheduler_array_task_id="",
                output_ready=False,
                reason="",
                ready_at="",
            )
            instances[instance.stage_instance_id] = instance
            if initial_state == INSTANCE_READY:
                dispatch_queue.append(instance.stage_instance_id)

    dependencies = _initial_dependencies(plan)
    return WorkflowState(
        pipeline_id=plan.pipeline_id,
        pipeline_kind=getattr(plan, "pipeline_kind", TRAIN_EVAL_PIPELINE_KIND),
        state=WORKFLOW_PLANNED,
        current_stage=plan.stage_order[0] if plan.stage_order else "",
        instances=instances,
        dependencies=dependencies,
        dispatch_queue=tuple(dispatch_queue),
        submissions={},
        release_policy=getattr(plan, "release_policy", "per_run"),
    )


def _initial_dependencies(plan: TrainEvalPipelinePlan) -> dict[str, DependencyState]:
    if TRAIN_STAGE not in plan.stage_batches or EVAL_STAGE not in plan.stage_batches:
        return {}
    train_by_run = {
        instance.run_id: instance.stage_instance_id
        for instance in plan.stage_batches[TRAIN_STAGE].stage_instances
    }
    dependencies: dict[str, DependencyState] = {}
    for eval_instance in plan.stage_batches[EVAL_STAGE].stage_instances:
        upstream = train_by_run.get(eval_instance.run_id)
        if not upstream:
            continue
        dependency = DependencyState(
            upstream_instance_id=upstream,
            downstream_instance_id=eval_instance.stage_instance_id,
            condition="success",
            state=DEPENDENCY_WAITING,
        )
        dependencies[dependency_key(upstream, eval_instance.stage_instance_id)] = dependency
    return dependencies
