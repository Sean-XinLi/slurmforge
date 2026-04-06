from __future__ import annotations

from typing import Any

from ..utils import read_schema_version
from .models import TrainOutputsManifest


def serialize_train_outputs_manifest(manifest: TrainOutputsManifest) -> dict[str, object]:
    return {
        "schema_version": int(manifest.schema_version),
        "run_id": manifest.run_id,
        "model_name": manifest.model_name,
        "result_dir": manifest.result_dir,
        "checkpoint_dir": manifest.checkpoint_dir,
        "primary_policy": manifest.primary_policy,
        "explicit_checkpoint": manifest.explicit_checkpoint,
        "primary_checkpoint": manifest.primary_checkpoint,
        "latest_checkpoint": manifest.latest_checkpoint,
        "best_checkpoint": manifest.best_checkpoint,
        "selection_reason": manifest.selection_reason,
        "selection_error": manifest.selection_error,
        "status": manifest.status,
    }


def deserialize_train_outputs_manifest(payload: dict[str, Any]) -> TrainOutputsManifest:
    if not isinstance(payload, dict):
        raise TypeError("Invalid train outputs manifest payload")
    return TrainOutputsManifest(
        schema_version=read_schema_version(payload),
        run_id=str(payload.get("run_id", "") or ""),
        model_name=str(payload.get("model_name", "") or ""),
        result_dir=str(payload.get("result_dir", "") or ""),
        checkpoint_dir=str(payload.get("checkpoint_dir", "") or ""),
        primary_policy=str(payload.get("primary_policy", "latest") or "latest"),
        explicit_checkpoint=str(payload.get("explicit_checkpoint", "") or ""),
        primary_checkpoint=str(payload.get("primary_checkpoint", "") or ""),
        latest_checkpoint=str(payload.get("latest_checkpoint", "") or ""),
        best_checkpoint=str(payload.get("best_checkpoint", "") or ""),
        selection_reason=str(payload.get("selection_reason", "") or ""),
        selection_error=str(payload.get("selection_error", "") or ""),
        status=str(payload.get("status", "ok") or "ok"),
    )
