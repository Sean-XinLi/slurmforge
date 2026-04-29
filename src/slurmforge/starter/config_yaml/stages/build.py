from __future__ import annotations

from typing import Any

from ...config_comments import comment_for
from ..scalar import scalar
from .entry import render_entry, render_launcher
from .inputs import render_inputs
from .outputs import render_outputs
from .resources import render_resources


def render_stages(lines: list[str], template_name: str, config: dict[str, Any]) -> None:
    lines.append("stages:")
    stages = config["stages"]
    for index, (stage_name, stage) in enumerate(stages.items()):
        if index:
            lines.append("")
        _render_stage(lines, template_name, stage_name, stage)


def _render_stage(
    lines: list[str],
    template_name: str,
    stage_name: str,
    stage: dict[str, Any],
) -> None:
    lines.extend(
        [
            f"  {stage_name}:",
            comment_for("stages.*.kind", indent=4),
            f"    kind: {scalar(stage['kind'])}",
            comment_for("stages.*.enabled", indent=4),
            f"    enabled: {scalar(stage['enabled'])}",
            comment_for("stages.*.environment", indent=4),
            f"    environment: {scalar(stage['environment'])}",
            comment_for("stages.*.runtime", indent=4),
            f"    runtime: {scalar(stage['runtime'])}",
        ]
    )
    render_entry(lines, stage["entry"])
    render_launcher(lines, stage["launcher"])
    render_resources(lines, stage["resources"], stage_name=stage_name)
    render_outputs(lines, stage["outputs"])
    if "depends_on" in stage:
        lines.extend(
            [
                comment_for("stages.*.depends_on", indent=4),
                "    depends_on:",
            ]
        )
        for item in stage["depends_on"]:
            lines.append(f"      - {scalar(item)}")
    if "inputs" in stage:
        render_inputs(lines, template_name, stage["inputs"])
