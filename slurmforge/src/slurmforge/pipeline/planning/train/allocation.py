from __future__ import annotations

import copy
from dataclasses import replace

from ....model_support.gpu_estimator import GpuEstimate
from ...config.runtime import ClusterConfig, ValidationConfig
from ..contracts import AllocationRequest, ExecutionTopology, ResourceEstimate
from ..enums import RuntimeProbe


def build_resource_estimate(estimate: GpuEstimate) -> ResourceEstimate:
    return ResourceEstimate(
        min_total_gpus=int(estimate.min_total_gpus),
        recommended_total_gpus=int(estimate.recommended_total_gpus),
        max_useful_total_gpus=int(estimate.max_useful_total_gpus),
        estimated_vram_gb=float(estimate.estimated_vram_gb),
        reason=str(estimate.reason),
    )


def resolve_runtime_probe(validation_cfg: ValidationConfig) -> RuntimeProbe:
    if str(validation_cfg.runtime_preflight or "error").strip().lower() == "off":
        return RuntimeProbe.NONE
    return RuntimeProbe.CUDA


def resolve_allocation(
    cluster_cfg: ClusterConfig,
    *,
    topology: ExecutionTopology,
    cluster_nodes_explicit: bool,
    cluster_gpus_per_node_explicit: bool,
) -> tuple[AllocationRequest, ClusterConfig]:
    nodes = int(cluster_cfg.nodes) if cluster_nodes_explicit else int(topology.nodes)
    if cluster_gpus_per_node_explicit:
        gpus_per_node = int(cluster_cfg.gpus_per_node or 1)
    else:
        gpus_per_node = int(topology.processes_per_node)
    allocation = AllocationRequest(
        nodes=nodes,
        gpus_per_node=gpus_per_node,
        cpus_per_task=int(cluster_cfg.cpus_per_task or 0),
        mem=str(cluster_cfg.mem),
    )
    resolved_cluster = replace(
        copy.deepcopy(cluster_cfg),
        nodes=allocation.nodes,
        gpus_per_node=allocation.gpus_per_node,
    )
    return allocation, resolved_cluster
