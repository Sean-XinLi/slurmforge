from __future__ import annotations

from typing import Any

from ..models.resources import AllocationRequest, ExecutionTopology, ResourceEstimate


def parse_resource_estimate(value: Any, *, name: str = "estimate") -> ResourceEstimate:
    if isinstance(value, ResourceEstimate):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    recommended = value.get("recommended_total_gpus", value.get("total_gpus", value.get("gpus", 1)))
    return ResourceEstimate(
        min_total_gpus=int(value.get("min_total_gpus", recommended) or recommended or 1),
        recommended_total_gpus=int(recommended or 1),
        max_useful_total_gpus=int(value.get("max_useful_total_gpus", recommended) or recommended or 1),
        estimated_vram_gb=float(value.get("estimated_vram_gb", 0.0) or 0.0),
        reason=str(value.get("reason", "") or ""),
    )


def serialize_resource_estimate(estimate: ResourceEstimate) -> dict[str, Any]:
    return {
        "min_total_gpus": estimate.min_total_gpus,
        "recommended_total_gpus": estimate.recommended_total_gpus,
        "max_useful_total_gpus": estimate.max_useful_total_gpus,
        "estimated_vram_gb": estimate.estimated_vram_gb,
        "reason": estimate.reason,
    }


def parse_execution_topology(value: Any, *, name: str = "topology") -> ExecutionTopology:
    if isinstance(value, ExecutionTopology):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return ExecutionTopology(
        nodes=int(value.get("nodes", value.get("nnodes", 1)) or 1),
        processes_per_node=int(value.get("processes_per_node", value.get("nproc_per_node", 1)) or 1),
        master_port=None if value.get("master_port") is None else int(value.get("master_port")),
    )


def serialize_execution_topology(topology: ExecutionTopology) -> dict[str, Any]:
    return {
        "nodes": topology.nodes,
        "processes_per_node": topology.processes_per_node,
        "master_port": topology.master_port,
        "total_processes": topology.total_processes,
    }


def parse_allocation_request(value: Any, *, name: str = "allocation") -> AllocationRequest:
    if isinstance(value, AllocationRequest):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return AllocationRequest(
        nodes=int(value.get("nodes", 1) or 1),
        gpus_per_node=int(value.get("gpus_per_node", 1) or 1),
        cpus_per_task=int(value.get("cpus_per_task", 0) or 0),
        mem=str(value.get("mem", "") or ""),
    )


def serialize_allocation_request(allocation: AllocationRequest) -> dict[str, Any]:
    return {
        "nodes": allocation.nodes,
        "gpus_per_node": allocation.gpus_per_node,
        "cpus_per_task": allocation.cpus_per_task,
        "mem": allocation.mem,
        "total_gpus": allocation.total_gpus,
    }
