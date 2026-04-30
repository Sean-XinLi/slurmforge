from __future__ import annotations

from typing import Any

from ...config_contract.registry import default_for


def stage_resources(*, cpus: int, mem: str) -> dict[str, Any]:
    return {
        "partition": default_for("stages.*.resources.partition"),
        "nodes": default_for("stages.*.resources.nodes"),
        "gpus_per_node": default_for("stages.*.resources.gpus_per_node"),
        "cpus_per_task": cpus,
        "mem": mem,
        "time_limit": default_for("stages.*.resources.time_limit"),
    }
