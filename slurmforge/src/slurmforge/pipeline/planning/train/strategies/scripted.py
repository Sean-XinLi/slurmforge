from __future__ import annotations

from pathlib import Path

from ....launch import build_stage_command, max_available_gpus
from ...contracts import ExecutionTopology, StageCapabilities, StageExecutionPlan
from ...enums import InvocationKind, LauncherKind, StageKind
from ..allocation import build_resource_estimate, resolve_allocation, resolve_runtime_probe
from ..context import TrainContext
from ..topology import resolve_effective_workdir, resolve_topology


def build_scripted_stage_plan(
    ctx: TrainContext,
    *,
    invocation_kind: InvocationKind,
    launcher_cfg,
    requested_mode: str,
    ddp_supported: bool,
    ddp_required: bool,
    script_path: Path,
    cli_args: dict[str, object],
    explicit_workdir: str | None,
) -> StageExecutionPlan:
    launcher_kind, resolved_launcher_cfg, _ = resolve_topology(
        requested_mode=requested_mode,
        launcher_cfg=launcher_cfg,
        cluster_cfg=ctx.cluster_cfg,
        launcher_nproc_per_node_explicit=ctx.launcher_nproc_per_node_explicit
        or launcher_cfg.distributed.nproc_per_node not in {None, ""},
        cluster_nodes_explicit=ctx.cluster_nodes_explicit,
        cluster_gpus_per_node_explicit=ctx.cluster_gpus_per_node_explicit,
        resources_cfg=ctx.resources_cfg,
        estimate=build_resource_estimate(ctx.estimate),
        ddp_supported=ddp_supported,
        ddp_required=ddp_required,
        run_index=ctx.run_index,
    )
    train_cmd, runtime = build_stage_command(
        script_path=script_path,
        args=cli_args,
        launcher_cfg=resolved_launcher_cfg,
        launch_mode="ddp" if launcher_kind == LauncherKind.DDP else "single",
        run_idx=ctx.run_index - 1,
    )
    topology = ExecutionTopology(
        nodes=runtime.nnodes,
        processes_per_node=runtime.nproc_per_node,
        master_port=runtime.master_port,
    )
    allocation, resolved_cluster_cfg = resolve_allocation(
        ctx.cluster_cfg,
        topology=topology,
        cluster_nodes_explicit=ctx.cluster_nodes_explicit,
        cluster_gpus_per_node_explicit=ctx.cluster_gpus_per_node_explicit,
    )
    workdir = resolve_effective_workdir(
        ctx.project_root,
        explicit_workdir,
        resolved_launcher_cfg.workdir,
    )
    return StageExecutionPlan(
        name="train",
        stage_kind=StageKind.TRAIN,
        invocation_kind=invocation_kind,
        launcher_kind=launcher_kind,
        command_text=train_cmd,
        workdir=workdir,
        topology=topology,
        allocation=allocation,
        estimate=build_resource_estimate(ctx.estimate),
        capabilities=StageCapabilities(
            ddp_supported=ddp_supported,
            ddp_required=ddp_required,
            uses_gpu=True,
            external_launcher=False,
            runtime_probe=resolve_runtime_probe(ctx.validation_cfg),
        ),
        python_bin=resolved_launcher_cfg.python_bin,
        launcher_cfg=resolved_launcher_cfg,
        cluster_cfg=resolved_cluster_cfg,
        script_path=script_path,
        cli_args=cli_args,
        command_mode=None,
        requested_launcher_mode=requested_mode,
        max_available_gpus_per_node=max_available_gpus(ctx.resources_cfg),
    )
