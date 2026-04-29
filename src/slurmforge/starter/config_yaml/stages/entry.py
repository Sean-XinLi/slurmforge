from __future__ import annotations

from typing import Any

from ...config_comments import comment_for, option_comment
from ..scalar import scalar


def render_entry(lines: list[str], entry: dict[str, Any]) -> None:
    lines.extend(
        [
            "    entry:",
            option_comment("stages.*.entry.type", indent=6),
            f"      type: {scalar(entry['type'])}",
            comment_for("stages.*.entry.script", indent=6),
            f"      script: {scalar(entry['script'])}",
            comment_for("stages.*.entry.workdir", indent=6),
            f"      workdir: {scalar(entry['workdir'])}",
            comment_for("stages.*.entry.args", indent=6),
            "      args:",
        ]
    )
    _render_mapping(lines, entry.get("args") or {}, indent=8)


def render_launcher(lines: list[str], launcher: dict[str, Any]) -> None:
    lines.extend(
        [
            "    launcher:",
            option_comment("stages.*.launcher.type", indent=6),
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
