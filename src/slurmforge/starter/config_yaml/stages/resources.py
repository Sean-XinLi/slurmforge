from __future__ import annotations

from typing import Any

from ..scalar import scalar


def render_resources(
    lines: list[str], resources: dict[str, Any], *, stage_name: str
) -> None:
    lines.extend(
        [
            "    resources:",
            f"      # Slurm partition/queue for each {stage_name} task.",
            f"      partition: {scalar(resources['partition'])}",
            f"      nodes: {scalar(resources['nodes'])}",
            '      # Integer GPU count, or "auto" when gpu_sizing is configured.',
            f"      gpus_per_node: {scalar(resources['gpus_per_node'])}",
            f"      cpus_per_task: {scalar(resources['cpus_per_task'])}",
            f"      mem: {scalar(resources['mem'])}",
            f"      time_limit: {scalar(resources['time_limit'])}",
        ]
    )
