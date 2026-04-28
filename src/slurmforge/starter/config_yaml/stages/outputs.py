from __future__ import annotations

from typing import Any

from ...config_comments import option_comment
from ..scalar import scalar


def render_outputs(lines: list[str], outputs: dict[str, Any]) -> None:
    lines.append("    outputs:")
    for output_name, output in outputs.items():
        lines.extend(
            [
                f"      {output_name}:",
                option_comment("stages.*.outputs.*.kind", indent=8),
                f"        kind: {scalar(output['kind'])}",
                f"        required: {scalar(output['required'])}",
            ]
        )
        if "discover" in output:
            _render_discovery(lines, output["discover"])
        if output["kind"] in {"metric", "manifest"}:
            lines.extend(
                [
                    "        # JSON file produced by the stage.",
                    f"        file: {scalar(output['file'])}",
                ]
            )
        if output["kind"] == "metric":
            lines.extend(
                [
                    "        # JSONPath for the metric value inside file.",
                    f"        json_path: {scalar(output['json_path'])}",
                ]
            )


def _render_discovery(lines: list[str], discover: dict[str, Any]) -> None:
    lines.extend(
        [
            "        discover:",
            "          # Glob patterns are evaluated under the stage run directory.",
            "          globs:",
        ]
    )
    for pattern in discover.get("globs", ()):
        lines.append(f"            - {scalar(pattern)}")
    if "select" in discover:
        lines.extend(
            [
                option_comment("stages.*.outputs.*.discover.select", indent=10),
                f"          select: {scalar(discover['select'])}",
            ]
        )
