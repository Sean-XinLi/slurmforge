from __future__ import annotations

from typing import Any

from ...utils import deep_merge
from ..runtime import ArtifactsConfig, DEFAULT_ARTIFACTS
from ..utils import ensure_dict
from .shared import ensure_normalized_config


def normalize_artifacts(cfg: dict[str, Any]) -> ArtifactsConfig:
    merged = deep_merge(DEFAULT_ARTIFACTS, ensure_dict(cfg, "artifacts"))
    return ArtifactsConfig(
        checkpoint_globs=[str(x) for x in list(merged.get("checkpoint_globs") or [])],
        eval_csv_globs=[str(x) for x in list(merged.get("eval_csv_globs") or [])],
        eval_image_globs=[str(x) for x in list(merged.get("eval_image_globs") or [])],
        extra_globs=[str(x) for x in list(merged.get("extra_globs") or [])],
    )


def ensure_artifacts_config(value: Any, name: str = "artifacts") -> ArtifactsConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=ArtifactsConfig,
        normalizer=normalize_artifacts,
    )
