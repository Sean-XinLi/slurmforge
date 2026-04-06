from __future__ import annotations

from typing import Any

from ..models import ClusterConfig


def serialize_cluster_config(config: ClusterConfig) -> dict[str, Any]:
    return {
        "partition": str(config.partition),
        "account": str(config.account),
        "qos": str(config.qos),
        "time_limit": str(config.time_limit),
        "nodes": int(config.nodes),
        "gpus_per_node": None if config.gpus_per_node is None else int(config.gpus_per_node),
        "cpus_per_task": int(config.cpus_per_task),
        "mem": str(config.mem),
        "constraint": str(config.constraint),
        "extra_sbatch_args": [str(item) for item in config.extra_sbatch_args],
    }
