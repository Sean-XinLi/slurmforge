from __future__ import annotations

from dataclasses import dataclass

from ....errors import PlanningError
from ...launch.types import LaunchRuntime


@dataclass(frozen=True)
class ResourceEstimate:
    min_total_gpus: int
    recommended_total_gpus: int
    max_useful_total_gpus: int
    estimated_vram_gb: float
    reason: str

    def __post_init__(self) -> None:
        min_total_gpus = max(1, int(self.min_total_gpus))
        recommended_total_gpus = max(min_total_gpus, int(self.recommended_total_gpus))
        max_useful_total_gpus = max(recommended_total_gpus, int(self.max_useful_total_gpus))
        estimated_vram_gb = float(self.estimated_vram_gb)
        reason = str(self.reason or "").strip()
        if not reason:
            raise PlanningError("ResourceEstimate.reason must be non-empty")
        object.__setattr__(self, "min_total_gpus", min_total_gpus)
        object.__setattr__(self, "recommended_total_gpus", recommended_total_gpus)
        object.__setattr__(self, "max_useful_total_gpus", max_useful_total_gpus)
        object.__setattr__(self, "estimated_vram_gb", estimated_vram_gb)
        object.__setattr__(self, "reason", reason)

    @property
    def total_gpus(self) -> int:
        return self.recommended_total_gpus


@dataclass(frozen=True)
class ExecutionTopology:
    nodes: int
    processes_per_node: int
    master_port: int | None = None

    def __post_init__(self) -> None:
        nodes = int(self.nodes)
        processes_per_node = int(self.processes_per_node)
        if nodes < 1:
            raise PlanningError("ExecutionTopology.nodes must be >= 1")
        if processes_per_node < 1:
            raise PlanningError("ExecutionTopology.processes_per_node must be >= 1")
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "processes_per_node", processes_per_node)
        object.__setattr__(self, "master_port", None if self.master_port is None else int(self.master_port))

    @property
    def total_processes(self) -> int:
        return self.nodes * self.processes_per_node

    @property
    def nnodes(self) -> int:
        return self.nodes

    @property
    def nproc_per_node(self) -> int:
        return self.processes_per_node

    def to_launch_runtime(self) -> LaunchRuntime:
        return LaunchRuntime(
            nnodes=self.nodes,
            nproc_per_node=self.processes_per_node,
            master_port=self.master_port,
        )


@dataclass(frozen=True)
class AllocationRequest:
    nodes: int
    gpus_per_node: int
    cpus_per_task: int
    mem: str

    def __post_init__(self) -> None:
        nodes = int(self.nodes)
        gpus_per_node = int(self.gpus_per_node)
        cpus_per_task = int(self.cpus_per_task)
        mem = str(self.mem or "").strip()
        if nodes < 1:
            raise PlanningError("AllocationRequest.nodes must be >= 1")
        if gpus_per_node < 1:
            raise PlanningError("AllocationRequest.gpus_per_node must be >= 1")
        if cpus_per_task < 0:
            raise PlanningError("AllocationRequest.cpus_per_task must be >= 0")
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "gpus_per_node", gpus_per_node)
        object.__setattr__(self, "cpus_per_task", cpus_per_task)
        object.__setattr__(self, "mem", mem)

    @property
    def total_gpus(self) -> int:
        return self.nodes * self.gpus_per_node
