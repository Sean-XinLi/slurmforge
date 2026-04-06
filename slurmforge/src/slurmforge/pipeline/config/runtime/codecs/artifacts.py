from __future__ import annotations

from typing import Any

from ..models import ArtifactsConfig


def serialize_artifacts_config(config: ArtifactsConfig) -> dict[str, Any]:
    return {
        "checkpoint_globs": [str(item) for item in config.checkpoint_globs],
        "eval_csv_globs": [str(item) for item in config.eval_csv_globs],
        "eval_image_globs": [str(item) for item in config.eval_image_globs],
        "extra_globs": [str(item) for item in config.extra_globs],
    }
