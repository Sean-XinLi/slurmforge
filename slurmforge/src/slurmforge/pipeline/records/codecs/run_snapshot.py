from __future__ import annotations

from typing import Any

from ...config.utils import ensure_dict
from ..models.run_snapshot import RunSnapshot
from ..replay_spec import ensure_replay_spec, serialize_replay_spec
from .metadata import ensure_generated_by, serialize_generated_by


def serialize_run_snapshot(snapshot: RunSnapshot) -> dict[str, Any]:
    return {
        "run_index": int(snapshot.run_index),
        "total_runs": int(snapshot.total_runs),
        "run_id": str(snapshot.run_id),
        "generated_by": serialize_generated_by(snapshot.generated_by),
        "project": str(snapshot.project),
        "experiment_name": str(snapshot.experiment_name),
        "model_name": str(snapshot.model_name),
        "train_mode": str(snapshot.train_mode),
        "sweep_case_name": None if snapshot.sweep_case_name in (None, "") else str(snapshot.sweep_case_name),
        "sweep_assignments": dict(snapshot.sweep_assignments),
        "replay_spec": serialize_replay_spec(snapshot.replay_spec),
    }


def deserialize_run_snapshot(payload: dict[str, Any]) -> RunSnapshot:
    if not isinstance(payload, dict):
        raise TypeError("run snapshot must be a mapping")
    return RunSnapshot(
        run_index=int(payload["run_index"]),
        total_runs=int(payload["total_runs"]),
        run_id=str(payload["run_id"]),
        generated_by=ensure_generated_by(payload.get("generated_by")),
        project=str(payload["project"]),
        experiment_name=str(payload["experiment_name"]),
        model_name=str(payload["model_name"]),
        train_mode=str(payload["train_mode"]),
        sweep_case_name=None if payload.get("sweep_case_name") in (None, "") else str(payload.get("sweep_case_name")),
        sweep_assignments=ensure_dict(payload.get("sweep_assignments"), "sweep_assignments"),
        replay_spec=ensure_replay_spec(payload.get("replay_spec", {})),
    )
