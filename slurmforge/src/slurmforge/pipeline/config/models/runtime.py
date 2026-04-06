from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PlanningHints:
    launcher_nproc_per_node_explicit: bool = False
    cluster_nodes_explicit: bool = False
    cluster_gpus_per_node_explicit: bool = False


@dataclass(frozen=True)
class ExternalRuntimeConfig:
    nnodes: int = 1
    nproc_per_node: int = 1
