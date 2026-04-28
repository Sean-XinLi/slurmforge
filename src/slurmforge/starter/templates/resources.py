from __future__ import annotations

from typing import Any

from ..defaults import DEFAULT_PARTITION


def stage_resources(*, cpus: int, mem: str) -> dict[str, Any]:
    return {
        "partition": DEFAULT_PARTITION,
        "nodes": 1,
        "gpus_per_node": 1,
        "cpus_per_task": cpus,
        "mem": mem,
        "time_limit": "01:00:00",
    }
