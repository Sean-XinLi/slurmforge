from __future__ import annotations

from typing import Any

from ...utils import deep_merge
from ..runtime import ClusterConfig, DEFAULT_CLUSTER
from ..utils import _is_auto_value, ensure_dict
from .shared import ensure_normalized_config


def normalize_cluster(cfg: dict[str, Any]) -> ClusterConfig:
    merged = deep_merge(DEFAULT_CLUSTER, ensure_dict(cfg, "cluster"))
    return ClusterConfig(
        partition=str(merged["partition"]),
        account=str(merged["account"]),
        qos=str(merged["qos"]),
        time_limit=str(merged["time_limit"]),
        nodes=int(merged["nodes"]),
        gpus_per_node=None if _is_auto_value(merged.get("gpus_per_node")) else int(merged["gpus_per_node"]),
        cpus_per_task=int(merged["cpus_per_task"]),
        mem=str(merged["mem"]),
        constraint=str(merged["constraint"]),
        extra_sbatch_args=[str(x) for x in list(merged.get("extra_sbatch_args") or [])],
    )


def ensure_cluster_config(value: Any, name: str = "cluster") -> ClusterConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=ClusterConfig,
        normalizer=normalize_cluster,
    )
