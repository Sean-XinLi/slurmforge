from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..storage.batch_registry import BatchRegistryRecord
from ..storage.runtime_batches import iter_runtime_batch_records
from ..storage.workflow import write_workflow_status
from ..submission.ledger import submitted_group_job_ids
from ..workflow_contract import WORKFLOW_TERMINAL_STATES
from .gate_ledger import submitted_gate_records
from .state_records import WorkflowState, workflow_state_to_dict


@dataclass(frozen=True)
class PipelineAdvanceResult:
    pipeline_root: str
    state: str
    submitted_stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    submitted_gate_job_ids: dict[str, str] = field(default_factory=dict)
    completed: bool = False


def submitted_gate_job_ids(pipeline_root: Path) -> dict[str, str]:
    return {
        key: str(payload["scheduler_job_id"])
        for key, payload in submitted_gate_records(pipeline_root).items()
        if payload.get("scheduler_job_id")
    }


def submitted_stage_job_ids(pipeline_root: Path) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for record in iter_runtime_batch_records(pipeline_root):
        job_ids = submitted_group_job_ids(Path(record.stage_batch_root))
        if not job_ids:
            continue
        stage_jobs[_stage_job_key(record)] = job_ids
    return stage_jobs


def result_from_state(
    pipeline_root: Path, state: WorkflowState
) -> PipelineAdvanceResult:
    return PipelineAdvanceResult(
        pipeline_root=str(pipeline_root),
        state=state.state,
        submitted_stage_job_ids=submitted_stage_job_ids(pipeline_root),
        submitted_gate_job_ids=submitted_gate_job_ids(pipeline_root),
        completed=state.state in WORKFLOW_TERMINAL_STATES,
    )


def set_workflow_status(
    pipeline_root: Path,
    state: WorkflowState,
    status: str,
    *,
    reason: str,
) -> None:
    write_workflow_status(
        pipeline_root,
        status,
        reason=reason,
        gate_jobs=submitted_gate_records(pipeline_root),
        stage_jobs=submitted_stage_job_ids(pipeline_root),
        train_groups=workflow_state_to_dict(state)["train_groups"],
        final_gate=workflow_state_to_dict(state)["final_gate"],
    )


def stage_key(stage_name: str, *, group_id: str | None = None) -> str:
    return stage_name if group_id is None else f"{stage_name}:{group_id}"


def _stage_job_key(record: BatchRegistryRecord) -> str:
    return stage_key(record.stage_name, group_id=record.shard_id or None)
