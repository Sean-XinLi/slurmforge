from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ..config.api import ExperimentSpec, serialize_replay_experiment_spec


def run_id(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(text).hexdigest()[:10]


def user_run_id_payload(
    spec: ExperimentSpec,
    *,
    train_mode: str,
    model_name: str,
    project_root: Path,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "train_mode": train_mode,
        "model_name": model_name,
    }
    spec_payload = serialize_replay_experiment_spec(spec, project_root=project_root)
    for section in ("model", "resolved_model_catalog", "run", "launcher", "resources", "eval", "artifacts"):
        value = spec_payload.get(section)
        if value is not None:
            payload[section] = value
    return payload
