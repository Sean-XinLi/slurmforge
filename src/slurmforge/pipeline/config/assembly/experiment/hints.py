from __future__ import annotations

from ...models import PlanningHints
from ...utils import ensure_dict
from .inputs import ExperimentSectionInputs


def build_planning_hints(inputs: ExperimentSectionInputs) -> PlanningHints:
    distributed_cfg = ensure_dict(inputs.launcher_cfg_raw.get("distributed"), "launcher.distributed")
    return PlanningHints(
        launcher_nproc_per_node_explicit=distributed_cfg.get("nproc_per_node") not in {None, "", "auto"},
        cluster_nodes_explicit=inputs.cluster_cfg_raw.get("nodes") not in {None, "", "auto"},
        cluster_gpus_per_node_explicit=inputs.cluster_cfg_raw.get("gpus_per_node") not in {None, "", "auto"},
    )
