from __future__ import annotations

import json
from typing import Any

from ...errors import InternalCompilerError
from ..config.normalize import ensure_cluster_config, ensure_env_config
from ..config.runtime import ClusterConfig, EnvConfig, serialize_cluster_config, serialize_env_config


SLURM_RESOURCE_GROUPING_KEYS = (
    "partition",
    "account",
    "qos",
    "time_limit",
    "nodes",
    "gpus_per_node",
    "cpus_per_task",
    "mem",
    "constraint",
    "extra_sbatch_args",
)

_RESOURCE_BUCKET_KEYS = (
    "nodes",
    "gpus_per_node",
    "cpus_per_task",
    "mem",
)

RUNTIME_ENV_GROUPING_KEYS = (
    "modules",
    "conda_activate",
    "venv_activate",
    "extra_env",
)


def _extract_cluster_fields(cluster: ClusterConfig | dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    cluster_cfg = ensure_cluster_config(cluster)
    cluster_dict = serialize_cluster_config(cluster_cfg)
    return {key: cluster_dict.get(key) for key in keys}


def _slurm_resource_grouping_fields(cluster: ClusterConfig | dict[str, Any]) -> dict[str, Any]:
    return _extract_cluster_fields(cluster, SLURM_RESOURCE_GROUPING_KEYS)


def _runtime_env_grouping_fields(env: EnvConfig | dict[str, Any]) -> dict[str, Any]:
    env_cfg = ensure_env_config(env)
    env_dict = serialize_env_config(env_cfg)
    return {key: env_dict.get(key) for key in RUNTIME_ENV_GROUPING_KEYS}


def array_grouping_fields(cluster: ClusterConfig | dict[str, Any], env: EnvConfig | dict[str, Any]) -> dict[str, Any]:
    return {
        "cluster": _slurm_resource_grouping_fields(cluster),
        "runtime_env": _runtime_env_grouping_fields(env),
    }


def array_group_signature(cluster: ClusterConfig | dict[str, Any], env: EnvConfig | dict[str, Any]) -> str:
    payload = array_grouping_fields(cluster, env)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def describe_array_group_reason() -> str:
    slurm_fields = ", ".join(SLURM_RESOURCE_GROUPING_KEYS)
    runtime_fields = ", ".join(f"env.{key}" for key in RUNTIME_ENV_GROUPING_KEYS)
    return (
        "grouped by identical Slurm resources ("
        + slurm_fields
        + ") and runtime environment bootstrap ("
        + runtime_fields
        + ")"
    )


def resource_request_from_cluster(cluster: ClusterConfig | dict[str, Any]) -> dict[str, Any]:
    return _slurm_resource_grouping_fields(cluster)


def resource_bucket_from_cluster(cluster: ClusterConfig | dict[str, Any]) -> dict[str, Any]:
    return _extract_cluster_fields(cluster, _RESOURCE_BUCKET_KEYS)


def summarize_resource_buckets(array_groups_meta: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for group in array_groups_meta:
        resource_bucket = resource_bucket_from_cluster(group["cluster"])
        bucket_key = json.dumps(resource_bucket, sort_keys=True, separators=(",", ":"))
        summary = buckets.get(bucket_key)
        if summary is None:
            summary = {
                "resource_request": resource_bucket,
                "total_tasks": 0,
                "group_count": 0,
                "group_indices": [],
            }
            buckets[bucket_key] = summary
        summary["total_tasks"] += int(group["array_size"])
        summary["group_count"] += 1
        summary["group_indices"].append(int(group["group_index"]))
    return list(buckets.values())


def ensure_cluster_renderable(cluster: ClusterConfig | dict[str, Any], *, context: str) -> None:
    cluster_cfg = ensure_cluster_config(cluster)
    if cluster_cfg.gpus_per_node in {None, ""}:
        raise InternalCompilerError(f"{context}: cluster.gpus_per_node is unresolved; cannot render sbatch --gres")
