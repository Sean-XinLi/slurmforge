from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClientProtocol
from .gate_ledger import gate_ledger_key, submit_gate_once
from .state import record_workflow_event, save_workflow_state
from .state_model import submitted_gate_job_ids
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
    key = gate_ledger_key(gate, target_id=target_id)
    existing_job_ids = submitted_gate_job_ids(pipeline_root)
    if key in existing_job_ids:
        return existing_job_ids[key]
    record = submit_gate_once(
        pipeline_root,
        plan,
        gate,
        dependency_job_ids=dependency_job_ids,
        client=client,
        target_id=target_id,
        target_kind=target_kind,
        max_dependency_length=max_dependency_length,
    )
    save_workflow_state(pipeline_root, state)
    record_workflow_event(
        pipeline_root,
        "pipeline_gate_submitted",
        gate=gate,
        target_kind=target_kind,
        target_id=target_id or "",
        gate_key=key,
        scheduler_job_id=record.scheduler_job_id,
        barrier_job_ids=list(record.barrier_job_ids),
        dependency_job_ids=list(dependency_job_ids),
    )
    return record.scheduler_job_id
