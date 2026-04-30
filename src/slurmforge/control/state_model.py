from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..storage.runtime_batches import iter_runtime_batch_records
from ..storage.workflow import write_workflow_status
from ..submission.ledger import submitted_group_job_ids
from .gate_ledger import submitted_gate_records


TRAIN_STAGE = "train"
EVAL_STAGE = "eval"


@dataclass(frozen=True)
class PipelineAdvanceResult:
    pipeline_root: str
    state: str
    submitted_stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    submitted_gate_job_ids: dict[str, str] = field(default_factory=dict)
    completed: bool = False


def train_groups(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = state.setdefault("train_groups", {})
    if not isinstance(value, dict):
        state["train_groups"] = {}
    return state["train_groups"]


def final_gate_state(state: dict[str, Any]) -> dict[str, Any]:
    value = state.setdefault("final_gate", {})
    if not isinstance(value, dict):
        state["final_gate"] = {}
    state["final_gate"].setdefault("state", "pending")
    return state["final_gate"]


def submitted_gate_job_ids(pipeline_root: Path) -> dict[str, str]:
    return {
        key: str(record["scheduler_job_id"])
        for key, record in submitted_gate_records(pipeline_root).items()
        if record.get("scheduler_job_id")
    }


def submitted_stage_job_ids(pipeline_root: Path) -> dict[str, dict[str, str]]:
    stage_jobs: dict[str, dict[str, str]] = {}
    for record in iter_runtime_batch_records(pipeline_root):
        root = record.get("stage_batch_root")
        if not root:
            continue
        job_ids = submitted_group_job_ids(Path(str(root)))
        if not job_ids:
            continue
        stage_jobs[_stage_job_key(record)] = job_ids
    return stage_jobs


def result_from_state(
    pipeline_root: Path, state: dict[str, Any]
) -> PipelineAdvanceResult:
    return PipelineAdvanceResult(
        pipeline_root=str(pipeline_root),
        state=str(state.get("state") or "unknown"),
        submitted_stage_job_ids=submitted_stage_job_ids(pipeline_root),
        submitted_gate_job_ids=submitted_gate_job_ids(pipeline_root),
        completed=str(state.get("state") or "") in {"success", "failed", "blocked"},
    )


def set_workflow_status(
    pipeline_root: Path,
    state: dict[str, Any],
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
        train_groups=train_groups(state),
        final_gate=final_gate_state(state),
    )


def stage_key(stage_name: str, *, group_id: str | None = None) -> str:
    return stage_name if group_id is None else f"{stage_name}:{group_id}"


def _stage_job_key(record: dict[str, Any]) -> str:
    stage_name = str(record.get("stage_name") or "")
    shard_id = str(record.get("shard_id") or "")
    return stage_key(stage_name, group_id=shard_id or None)
