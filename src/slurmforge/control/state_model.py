from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .gate_ledger import submitted_gate_records
from .state import save_workflow_state
from ..storage.workflow import write_workflow_status


TRAIN_STAGE = "train"
EVAL_STAGE = "eval"


@dataclass(frozen=True)
class PipelineAdvanceResult:
    pipeline_root: str
    state: str
    submitted_stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    submitted_gate_job_ids: dict[str, str] = field(default_factory=dict)
    completed: bool = False


def submitted_stages(state: dict[str, Any]) -> dict[str, dict[str, str]]:
    value = state.setdefault("submitted_stages", {})
    if not isinstance(value, dict):
        state["submitted_stages"] = {}
    return state["submitted_stages"]


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


def result_from_state(
    pipeline_root: Path, state: dict[str, Any]
) -> PipelineAdvanceResult:
    return PipelineAdvanceResult(
        pipeline_root=str(pipeline_root),
        state=str(state.get("state") or "unknown"),
        submitted_stage_job_ids={
            stage: dict(job_ids)
            for stage, job_ids in submitted_stages(state).items()
            if isinstance(job_ids, dict)
        },
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
        submitted_stages=submitted_stages(state),
        train_groups=train_groups(state),
        final_gate=final_gate_state(state),
    )


def stage_key(stage_name: str, *, group_id: str | None = None) -> str:
    return stage_name if group_id is None else f"{stage_name}:{group_id}"


def record_stage_jobs(
    pipeline_root: Path,
    state: dict[str, Any],
    stage_name: str,
    group_job_ids: dict[str, str],
    *,
    group_id: str | None = None,
) -> None:
    submitted_stages(state)[stage_key(stage_name, group_id=group_id)] = dict(
        group_job_ids
    )
    save_workflow_state(pipeline_root, state)
