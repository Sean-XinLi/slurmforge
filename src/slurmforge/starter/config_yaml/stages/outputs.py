from __future__ import annotations

from typing import Any

from ....config_contract.options import OUTPUT_KIND_MANIFEST, OUTPUT_KIND_METRIC
from ...config_comments import comment_for, option_comment
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
        if output["kind"] in {OUTPUT_KIND_METRIC, OUTPUT_KIND_MANIFEST}:
            lines.extend(
                [
                    comment_for("stages.*.outputs.*.file", indent=8),
                    f"        file: {scalar(output['file'])}",
                ]
            )
        if output["kind"] == OUTPUT_KIND_METRIC:
            lines.extend(
                [
                    comment_for("stages.*.outputs.*.json_path", indent=8),
                    f"        json_path: {scalar(output['json_path'])}",
                ]
            )


def _render_discovery(lines: list[str], discover: dict[str, Any]) -> None:
    lines.extend(
        [
            "        discover:",
            comment_for("stages.*.outputs.*.discover.globs", indent=10),
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
