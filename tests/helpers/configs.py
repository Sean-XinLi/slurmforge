from __future__ import annotations

from pathlib import Path

import yaml

from slurmforge.defaults import DEFAULT_CONFIG_FILENAME, TEMPLATE_TRAIN_EVAL
from slurmforge.starter import InitRequest, create_starter_project

from .overlays import apply_overlay
from .profiles import (
    DEFAULT_PROFILE,
    DEFAULT_REPLACE_SECTIONS,
    PROFILE_REPLACE_SECTIONS,
    profile_overlay,
)


def write_demo_project(
    root: Path,
    *,
    profile: str = DEFAULT_PROFILE,
    extra: dict | None = None,
    replace_sections: tuple[str, ...] = DEFAULT_REPLACE_SECTIONS,
) -> Path:
    cfg_path = root / DEFAULT_CONFIG_FILENAME
    create_starter_project(
        InitRequest(template=TEMPLATE_TRAIN_EVAL, output_dir=root, force=True)
    )
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    cfg = apply_overlay(
        cfg,
        profile_overlay(profile, root),
        replace_sections=PROFILE_REPLACE_SECTIONS,
    )
    if extra:
        cfg = apply_overlay(cfg, extra, replace_sections=replace_sections)
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return cfg_path
