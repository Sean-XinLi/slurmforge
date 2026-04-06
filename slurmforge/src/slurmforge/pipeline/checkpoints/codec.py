from __future__ import annotations

from ..utils import read_schema_version
from .models import CheckpointState


def serialize_checkpoint_state(state: CheckpointState) -> dict[str, object]:
    return {
        "schema_version": int(state.schema_version),
        "latest_checkpoint_rel": state.latest_checkpoint_rel,
        "selection_reason": state.selection_reason,
        "global_step": None if state.global_step is None else int(state.global_step),
    }


def deserialize_checkpoint_state(payload: dict[str, object]) -> CheckpointState:
    return CheckpointState(
        schema_version=read_schema_version(payload),
        latest_checkpoint_rel=str(payload.get("latest_checkpoint_rel", "") or ""),
        selection_reason=str(payload.get("selection_reason", "") or ""),
        global_step=None if payload.get("global_step") is None else int(payload.get("global_step")),
    )
