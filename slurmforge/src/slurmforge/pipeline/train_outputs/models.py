from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrainOutputsManifest:
    schema_version: int = 1
    run_id: str = ""
    model_name: str = ""
    result_dir: str = ""
    checkpoint_dir: str = ""
    primary_policy: str = "latest"
    explicit_checkpoint: str = ""
    primary_checkpoint: str = ""
    latest_checkpoint: str = ""
    best_checkpoint: str = ""
    selection_reason: str = ""
    selection_error: str = ""
    status: str = "ok"
