from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClientProtocol
from ..workflow_contract import WORKFLOW_STREAMING
from .dispatch_queue import dispatch_ready_instances
from .state import load_workflow_state, save_workflow_state
from .state_model import PipelineAdvanceResult, result_from_state, set_workflow_status


def submit_initial_pipeline_locked(
    plan,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    pipeline_root = Path(plan.root_dir).resolve()
    state = load_workflow_state(pipeline_root, plan)
    dispatch_ready_instances(
        pipeline_root,
        plan,
        state,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    save_workflow_state(pipeline_root, state)
    set_workflow_status(
        pipeline_root,
        state,
        WORKFLOW_STREAMING,
        reason="ready stage instances dispatched",
    )
    return result_from_state(pipeline_root, state)
