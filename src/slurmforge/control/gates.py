from __future__ import annotations

from pathlib import Path

from ..emit.pipeline_gate import (
    write_pipeline_gate_barrier_file,
    write_pipeline_gate_submit_file,
)
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import submit_dependent_job_with_dependency_tree
from ..workflow_contract import DISPATCH_CATCHUP_GATE
from .control_submission_ledger import submitted_control_job_ids
from .control_submission_records import (
    CONTROL_KIND_DISPATCH_CATCHUP_GATE,
    ControlSubmitResult,
    control_submission_key,
)
from .control_submission_submit import (
    submit_control_once,
)
from .state import record_workflow_event, save_workflow_state
from .state_records import WorkflowState


def submit_control_gate(
    pipeline_root: Path,
    state: WorkflowState,
    plan,
    gate: str,
    *,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    target_id: str | None = None,
    target_kind: str = "",
    max_dependency_length: int,
) -> str:
    key = _control_gate_key(gate, target_id=target_id)
    existing_job_ids = submitted_control_job_ids(pipeline_root)
    if key in existing_job_ids:
        return existing_job_ids[key][0]
    gate_path = write_pipeline_gate_submit_file(plan, gate, target_id=target_id)
    record = submit_control_once(
        pipeline_root,
        key=key,
        kind=CONTROL_KIND_DISPATCH_CATCHUP_GATE,
        target_kind=target_kind,
        target_id=target_id or "",
        sbatch_paths=(gate_path,),
        dependency_job_ids=dependency_job_ids,
        submitter=lambda: _submit_gate_with_dependencies(
            plan,
            gate,
            target_id=target_id,
            gate_path=gate_path,
            dependency_job_ids=dependency_job_ids,
            client=client,
            max_dependency_length=max_dependency_length,
        ),
    )
    save_workflow_state(pipeline_root, state)
    record_workflow_event(
        pipeline_root,
        "pipeline_gate_submitted",
        gate=gate,
        target_kind=target_kind,
        target_id=target_id or "",
        control_key=key,
        scheduler_job_id=record.scheduler_job_ids[0],
        barrier_job_ids=list(record.barrier_job_ids),
        dependency_job_ids=list(dependency_job_ids),
    )
    return record.scheduler_job_ids[0]


def _control_gate_key(gate: str, *, target_id: str | None) -> str:
    if gate != DISPATCH_CATCHUP_GATE:
        raise ValueError(f"Unsupported control gate: {gate}")
    return control_submission_key(
        CONTROL_KIND_DISPATCH_CATCHUP_GATE,
        target_id=target_id or "all",
    )


def _submit_gate_with_dependencies(
    plan,
    gate: str,
    *,
    target_id: str | None,
    gate_path: Path,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> ControlSubmitResult:
    gate_job_id, barrier_job_ids = submit_dependent_job_with_dependency_tree(
        target_path=gate_path,
        dependency_job_ids=dependency_job_ids,
        client=client,
        max_dependency_length=max_dependency_length,
        barrier_path_factory=lambda barrier_index: write_pipeline_gate_barrier_file(
            plan,
            gate,
            target_id=target_id,
            barrier_index=barrier_index,
        ),
    )
    return ControlSubmitResult(
        scheduler_job_ids=(gate_job_id,),
        barrier_job_ids=barrier_job_ids,
    )
