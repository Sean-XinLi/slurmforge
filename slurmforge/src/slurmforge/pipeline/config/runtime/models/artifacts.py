from __future__ import annotations

from dataclasses import dataclass, field


def default_checkpoint_globs() -> list[str]:
    return [
        "checkpoints/**",
        "saved_Model/**",
        "**/*.pt",
        "**/*.ckpt",
        "**/*.bin",
    ]


def default_eval_csv_globs() -> list[str]:
    return [
        "summary/**/*.csv",
        "**/*eval*.csv",
        "**/*loss*.csv",
    ]


def default_eval_image_globs() -> list[str]:
    return [
        "**/*.png",
        "**/*.jpg",
        "**/*.jpeg",
        "**/*.svg",
        "**/*.pdf",
    ]


def default_extra_globs() -> list[str]:
    return []


@dataclass(frozen=True)
class ArtifactsConfig:
    checkpoint_globs: list[str] = field(default_factory=default_checkpoint_globs)
    eval_csv_globs: list[str] = field(default_factory=default_eval_csv_globs)
    eval_image_globs: list[str] = field(default_factory=default_eval_image_globs)
    extra_globs: list[str] = field(default_factory=default_extra_globs)
