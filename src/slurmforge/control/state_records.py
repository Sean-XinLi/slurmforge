from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..io import SchemaVersion, require_schema, to_jsonable
from ..workflow_contract import (
    TRAIN_GROUP_EVAL_GATE_SUBMITTED,
    TRAIN_GROUP_EVAL_MATERIALIZED,
    TRAIN_GROUP_EVAL_MISSING,
    TRAIN_GROUP_EVAL_SKIPPED,
    TRAIN_GROUP_GATE_SUBMITTED,
    TRAIN_GROUP_RECONCILED,
    TRAIN_GROUP_TERMINAL,
    TRAIN_GROUP_TRAIN_SUBMITTED,
    TRAIN_GROUP_WAITING_EVAL,
    TRAIN_GROUP_WAITING_TRAIN,
)


TRAIN_GROUP_STATES = frozenset(
    {
        TRAIN_GROUP_TRAIN_SUBMITTED,
        TRAIN_GROUP_GATE_SUBMITTED,
        TRAIN_GROUP_WAITING_TRAIN,
        TRAIN_GROUP_RECONCILED,
        TRAIN_GROUP_EVAL_MATERIALIZED,
        TRAIN_GROUP_EVAL_GATE_SUBMITTED,
        TRAIN_GROUP_EVAL_MISSING,
        TRAIN_GROUP_WAITING_EVAL,
        TRAIN_GROUP_EVAL_SKIPPED,
        TRAIN_GROUP_TERMINAL,
    }
)


@dataclass
class TrainGroupState:
    group_id: str
    state: str
    run_ids: tuple[str, ...]
    stage_instance_ids: tuple[str, ...]
    train_group_gate_key: str = ""
    eval_shard_root: str = ""
    eval_shard_gate_key: str = ""
    terminal_dependency_gate_key: str = ""
    eval_shard_group_count: int = 0


@dataclass
class FinalGateState:
    state: str = "pending"
    gate_key: str = ""
    dependency_job_ids: tuple[str, ...] = ()


@dataclass
class WorkflowState:
    pipeline_id: str
    pipeline_kind: str
    state: str
    current_stage: str | None
    train_groups: dict[str, TrainGroupState] = field(default_factory=dict)
    final_gate: FinalGateState = field(default_factory=FinalGateState)
    schema_version: int = SchemaVersion.WORKFLOW_STATE


def workflow_state_from_dict(payload: dict[str, Any]) -> WorkflowState:
    version = require_schema(
        payload, name="workflow_state", version=SchemaVersion.WORKFLOW_STATE
    )
    groups_raw = dict(payload["train_groups"])
    return WorkflowState(
        schema_version=version,
        pipeline_id=str(payload["pipeline_id"]),
        pipeline_kind=str(payload["pipeline_kind"]),
        state=str(payload["state"]),
        current_stage=None
        if payload.get("current_stage") in (None, "")
        else str(payload["current_stage"]),
        train_groups={
            group_id: train_group_state_from_dict(group_id, dict(record))
            for group_id, record in groups_raw.items()
        },
        final_gate=final_gate_state_from_dict(dict(payload["final_gate"])),
    )


def workflow_state_to_dict(state: WorkflowState) -> dict[str, Any]:
    return to_jsonable(state)


def train_group_state_from_dict(
    group_id: str, payload: dict[str, Any]
) -> TrainGroupState:
    state = str(payload["state"])
    if state not in TRAIN_GROUP_STATES:
        raise ValueError(f"Unsupported train group state `{state}` for `{group_id}`")
    return TrainGroupState(
        group_id=group_id,
        state=state,
        run_ids=tuple(str(item) for item in payload["run_ids"]),
        stage_instance_ids=tuple(str(item) for item in payload["stage_instance_ids"]),
        train_group_gate_key=str(payload.get("train_group_gate_key") or ""),
        eval_shard_root=str(payload.get("eval_shard_root") or ""),
        eval_shard_gate_key=str(payload.get("eval_shard_gate_key") or ""),
        terminal_dependency_gate_key=str(
            payload.get("terminal_dependency_gate_key") or ""
        ),
        eval_shard_group_count=int(payload.get("eval_shard_group_count") or 0),
    )


def final_gate_state_from_dict(payload: dict[str, Any]) -> FinalGateState:
    return FinalGateState(
        state=str(payload["state"]),
        gate_key=str(payload.get("gate_key") or ""),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids") or ()
        ),
    )


def set_train_group(state: WorkflowState, group: TrainGroupState) -> None:
    state.train_groups[group.group_id] = group
