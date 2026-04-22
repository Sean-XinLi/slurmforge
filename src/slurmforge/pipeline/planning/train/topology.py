from __future__ import annotations

import copy
import math
from dataclasses import replace
from pathlib import Path

from ....errors import PlanningError
from ...config.runtime import LauncherConfig, ResourcesConfig
from ...config.utils import resolve_path
from ...launch import build_stage_command, max_gpus_per_job
from ..contracts import ExecutionTopology, ResourceEstimate
from ..enums import LauncherKind


def resolve_effective_workdir(project_root: Path, explicit_workdir: str | None, launcher_workdir: str) -> Path:
    base_workdir = resolve_path(project_root, launcher_workdir, ".")
    return resolve_path(project_root, explicit_workdir, str(base_workdir))


def resolve_launcher_kind(
    requested_mode: str,
    *,
    ddp_supported: bool,
    ddp_required: bool,
    recommended_total_gpus: int,
) -> LauncherKind:
    requested = (requested_mode or "auto").strip().lower()
    if requested not in {"auto", "ddp", "single"}:
        raise PlanningError("launcher.mode must be one of: auto, ddp, single")
    if requested == "single":
        return LauncherKind.SINGLE
    if requested == "ddp":
        return LauncherKind.DDP
    if ddp_required:
        return LauncherKind.DDP
    if recommended_total_gpus > 1 and ddp_supported:
        return LauncherKind.DDP
    return LauncherKind.SINGLE


def resolve_topology(
    *,
    requested_mode: str,
    launcher_cfg: LauncherConfig,
    cluster_cfg,
    launcher_nproc_per_node_explicit: bool,
    cluster_nodes_explicit: bool,
    cluster_gpus_per_node_explicit: bool,
    resources_cfg: ResourcesConfig,
    estimate: ResourceEstimate,
    ddp_supported: bool,
    ddp_required: bool,
    run_index: int,
) -> tuple[LauncherKind, LauncherConfig, ExecutionTopology]:
    launcher_kind = resolve_launcher_kind(
        requested_mode,
        ddp_supported=ddp_supported,
        ddp_required=ddp_required,
        recommended_total_gpus=estimate.recommended_total_gpus,
    )

    if launcher_kind == LauncherKind.SINGLE:
        resolved_launcher = replace(
            copy.deepcopy(launcher_cfg),
            mode="single",
            distributed=replace(
                copy.deepcopy(launcher_cfg.distributed),
                nnodes=1,
                nproc_per_node=1,
            ),
        )
        _, runtime = build_stage_command(
            script_path=Path("placeholder.py"),
            args={},
            launcher_cfg=resolved_launcher,
            launch_mode="single",
            run_idx=run_index - 1,
        )
        topology = ExecutionTopology(
            nodes=runtime.nnodes,
            processes_per_node=runtime.nproc_per_node,
            master_port=runtime.master_port,
        )
        return launcher_kind, resolved_launcher, topology

    distributed_cfg = copy.deepcopy(launcher_cfg.distributed)
    per_node_limit = (
        int(cluster_cfg.gpus_per_node or 1)
        if cluster_gpus_per_node_explicit and cluster_cfg.gpus_per_node not in {None, ""}
        else max_gpus_per_job(resources_cfg)
    )
    desired_total = max(1, int(estimate.recommended_total_gpus))
    configured_nodes = max(1, int(distributed_cfg.nnodes or 1))
    configured_nproc = None if distributed_cfg.nproc_per_node in {None, ""} else int(distributed_cfg.nproc_per_node)

    if launcher_nproc_per_node_explicit and configured_nproc is not None:
        processes_per_node = configured_nproc
        if configured_nodes > 1:
            nodes = configured_nodes
        else:
            nodes = max(1, math.ceil(desired_total / max(processes_per_node, 1)))
    elif cluster_nodes_explicit:
        nodes = max(1, int(cluster_cfg.nodes or 1))
        processes_per_node = max(1, math.ceil(desired_total / nodes))
    elif configured_nodes > 1:
        nodes = configured_nodes
        processes_per_node = max(1, math.ceil(desired_total / nodes))
    else:
        processes_per_node = max(1, min(per_node_limit, desired_total))
        nodes = max(1, math.ceil(desired_total / max(processes_per_node, 1)))

    resolved_launcher = replace(
        copy.deepcopy(launcher_cfg),
        mode="ddp",
        distributed=replace(
            distributed_cfg,
            nnodes=nodes,
            nproc_per_node=processes_per_node,
        ),
    )
    _, runtime = build_stage_command(
        script_path=Path("placeholder.py"),
        args={},
        launcher_cfg=resolved_launcher,
        launch_mode="ddp",
        run_idx=run_index - 1,
    )
    topology = ExecutionTopology(
        nodes=runtime.nnodes,
        processes_per_node=runtime.nproc_per_node,
        master_port=runtime.master_port,
    )
    return launcher_kind, resolved_launcher, topology
