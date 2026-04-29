from __future__ import annotations

from typing import Any

from ...config_comments import comment_for
from ..scalar import scalar


def render_resources(
    lines: list[str], resources: dict[str, Any], *, stage_name: str
) -> None:
    lines.extend(
        [
            "    resources:",
            comment_for("stages.*.resources.partition", indent=6),
            f"      partition: {scalar(resources['partition'])}",
            comment_for("stages.*.resources.nodes", indent=6),
            f"      nodes: {scalar(resources['nodes'])}",
            comment_for("stages.*.resources.gpus_per_node", indent=6),
            f"      gpus_per_node: {scalar(resources['gpus_per_node'])}",
            comment_for("stages.*.resources.cpus_per_task", indent=6),
            f"      cpus_per_task: {scalar(resources['cpus_per_task'])}",
            comment_for("stages.*.resources.mem", indent=6),
            f"      mem: {scalar(resources['mem'])}",
            comment_for("stages.*.resources.time_limit", indent=6),
            f"      time_limit: {scalar(resources['time_limit'])}",
        ]
    )
