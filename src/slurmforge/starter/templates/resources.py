from __future__ import annotations

from typing import Any

from ...config_contract.defaults import (
    DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE,
    DEFAULT_STAGE_RESOURCES_NODES,
    DEFAULT_STAGE_RESOURCES_PARTITION,
    DEFAULT_STAGE_RESOURCES_TIME_LIMIT,
)


def stage_resources(*, cpus: int, mem: str) -> dict[str, Any]:
    return {
        "partition": DEFAULT_STAGE_RESOURCES_PARTITION,
        "nodes": DEFAULT_STAGE_RESOURCES_NODES,
        "gpus_per_node": DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE,
        "cpus_per_task": cpus,
        "mem": mem,
        "time_limit": DEFAULT_STAGE_RESOURCES_TIME_LIMIT,
    }
