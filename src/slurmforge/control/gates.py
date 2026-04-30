from __future__ import annotations

from pathlib import Path
from typing import Any

from ..slurm import SlurmClientProtocol
from .gate_ledger import gate_ledger_key, submit_gate_once
from .state import record_workflow_event, save_workflow_state
from .state_model import submitted_gate_job_ids


def submit_control_gate(
    pipeline_root: Path,
    state: dict[str, Any],
    plan,
    gate: str,
    *,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    group_id: str | None = None,
    max_dependency_length: int,
) -> str:
    key = gate_ledger_key(gate, group_id=group_id)
    existing_job_ids = submitted_gate_job_ids(pipeline_root)
    if key in existing_job_ids:
        return existing_job_ids[key]
    record = submit_gate_once(
        pipeline_root,
        plan,
        gate,
        dependency_job_ids=dependency_job_ids,
        client=client,
        group_id=group_id,
        max_dependency_length=max_dependency_length,
    )
    save_workflow_state(pipeline_root, state)
    record_workflow_event(
        pipeline_root,
        "pipeline_gate_submitted",
        gate=gate,
        group_id=group_id or "",
        gate_key=key,
        scheduler_job_id=record.scheduler_job_id,
        barrier_job_ids=list(record.barrier_job_ids),
        dependency_job_ids=list(dependency_job_ids),
    )
    return record.scheduler_job_id
