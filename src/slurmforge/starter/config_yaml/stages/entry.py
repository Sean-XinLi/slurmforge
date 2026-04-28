from __future__ import annotations

from typing import Any

from ...config_comments import option_comment
from ..scalar import scalar


def render_entry(lines: list[str], entry: dict[str, Any]) -> None:
    lines.extend(
        [
            "    entry:",
            option_comment("stages.*.entry.type", indent=6),
            f"      type: {scalar(entry['type'])}",
            "      # Script path is resolved relative to this YAML file's directory.",
            f"      script: {scalar(entry['script'])}",
            f"      workdir: {scalar(entry['workdir'])}",
            "      # Each key is passed to the script as a CLI flag.",
            "      args:",
        ]
    )
    _render_mapping(lines, entry.get("args") or {}, indent=8)


def render_launcher(lines: list[str], launcher: dict[str, Any]) -> None:
    lines.extend(
        [
            "    launcher:",
            option_comment("stages.*.launcher.type", indent=6),
            "      # single is best for normal one-process Python scripts.",
            f"      type: {scalar(launcher['type'])}",
        ]
    )


def _render_mapping(lines: list[str], mapping: dict[str, Any], *, indent: int) -> None:
    if not mapping:
        lines.append(f"{' ' * indent}{{}}")
        return
    prefix = " " * indent
    for key, value in mapping.items():
        lines.append(f"{prefix}{key}: {scalar(value)}")
