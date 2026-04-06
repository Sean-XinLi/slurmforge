from __future__ import annotations

import copy
from dataclasses import replace

from ....launch import max_available_gpus
from ...contracts import ExecutionTopology, StageCapabilities, StageExecutionPlan
from ...enums import InvocationKind, LauncherKind, StageKind
from ...external_command import resolve_external_command_text
from ..allocation import build_resource_estimate, resolve_allocation, resolve_runtime_probe
from ..context import TrainContext
from ..topology import resolve_effective_workdir
from .base import TrainModeStrategy


class CommandTrainStrategy(TrainModeStrategy):
    mode = "command"

    def build(self, ctx: TrainContext) -> StageExecutionPlan:
        command_text, command_mode = resolve_external_command_text(
            str(ctx.run_spec.command or ""),
            command_mode=ctx.run_spec.command_mode,
            command_field_name="run.command",
            mode_field_name="run.command_mode",
            resume_from_checkpoint=ctx.run_spec.resume_from_checkpoint,
        )

        external_runtime = ctx.run_spec.external_runtime
        topology = ExecutionTopology(
            nodes=int(external_runtime.nnodes),
            processes_per_node=int(external_runtime.nproc_per_node),
            master_port=None,
        )
        allocation, resolved_cluster_cfg = resolve_allocation(
            ctx.cluster_cfg,
            topology=topology,
            cluster_nodes_explicit=ctx.cluster_nodes_explicit,
            cluster_gpus_per_node_explicit=ctx.cluster_gpus_per_node_explicit,
        )
        resolved_launcher_cfg = replace(
            copy.deepcopy(ctx.launcher_cfg),
            mode="ddp" if topology.total_processes > 1 else "single",
            distributed=replace(
                copy.deepcopy(ctx.launcher_cfg.distributed),
                nnodes=topology.nodes,
                nproc_per_node=topology.processes_per_node,
            ),
        )
        workdir = resolve_effective_workdir(
            ctx.project_root,
            ctx.run_spec.workdir,
            ctx.launcher_cfg.workdir,
        )
        return StageExecutionPlan(
            name="train",
            stage_kind=StageKind.TRAIN,
            invocation_kind=InvocationKind.EXTERNAL_COMMAND,
            launcher_kind=LauncherKind.EXTERNAL,
            command_text=command_text,
            workdir=workdir,
            topology=topology,
            allocation=allocation,
            estimate=build_resource_estimate(ctx.estimate),
            capabilities=StageCapabilities(
                ddp_supported=True,
                ddp_required=False,
                uses_gpu=True,
                external_launcher=True,
                runtime_probe=resolve_runtime_probe(ctx.validation_cfg),
            ),
            python_bin=ctx.launcher_cfg.python_bin,
            launcher_cfg=resolved_launcher_cfg,
            cluster_cfg=resolved_cluster_cfg,
            script_path=None,
            cli_args={},
            command_mode=command_mode,
            requested_launcher_mode="external",
            max_available_gpus_per_node=max_available_gpus(ctx.resources_cfg),
        )
