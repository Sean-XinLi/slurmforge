from __future__ import annotations

from typing import Any


def normalize_stage_shorthand_path(raw: dict[str, Any], path: str) -> str:
    first = path.split(".", 1)[0]
    stages = raw.get("stages")
    if isinstance(stages, dict) and first in stages and not path.startswith("stages."):
        return f"stages.{path}"
    return path


def normalize_run_override_path(raw: dict[str, Any], path: str) -> str:
    return normalize_stage_shorthand_path(raw, path)


def normalize_cli_override_path(raw: dict[str, Any], path: str) -> str:
    return normalize_stage_shorthand_path(raw, path)
