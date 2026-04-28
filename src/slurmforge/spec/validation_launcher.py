from __future__ import annotations

from ..errors import ConfigContractError
from ..sizing.models import GpuSizingResolution
from ..field_options import options_for, options_sentence
from .models import StageSpec
from .validation_common import explicit_int, require_port


def validate_launcher_contract(stage: StageSpec, *, sizing_resolution: GpuSizingResolution) -> None:
    if stage.launcher.type not in options_for("stages.*.launcher.type"):
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type` must be "
            f"{options_sentence('stages.*.launcher.type')}"
        )
    if stage.launcher.type == "torchrun":
        _validate_torchrun_launcher(stage, sizing_resolution=sizing_resolution)
    if stage.entry.type == "command" and stage.launcher.type not in {"single", "command", "srun", "mpirun"}:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type={stage.launcher.type}` cannot wrap command entries"
        )


def _validate_torchrun_launcher(stage: StageSpec, *, sizing_resolution: GpuSizingResolution) -> None:
    if stage.entry.type != "python_script":
        raise ConfigContractError(f"`stages.{stage.name}.launcher.type=torchrun` requires a python_script entry")
    mode = str(stage.launcher.options.get("mode") or ("multi_node" if stage.resources.nodes > 1 else "single_node"))
    if mode not in options_for("stages.*.launcher.mode"):
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.mode` must be "
            f"{options_sentence('stages.*.launcher.mode')}"
        )
    if mode == "single_node" and stage.resources.nodes != 1:
        raise ConfigContractError(f"`stages.{stage.name}.launcher.mode=single_node` requires resources.nodes == 1")
    if mode == "multi_node" and stage.resources.nodes < 2:
        raise ConfigContractError(f"`stages.{stage.name}.launcher.mode=multi_node` requires resources.nodes >= 2")
    nnodes = explicit_int(
        stage.launcher.options.get("nnodes", stage.launcher.options.get("nodes", "auto")),
        field=f"stages.{stage.name}.launcher.nnodes",
    )
    if nnodes is not None and nnodes != stage.resources.nodes:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.nnodes` must equal resources.nodes ({stage.resources.nodes})"
        )
    nproc_per_node = explicit_int(
        stage.launcher.options.get(
            "nproc_per_node",
            stage.launcher.options.get("processes_per_node", "auto"),
        ),
        field=f"stages.{stage.name}.launcher.nproc_per_node",
    )
    if nproc_per_node is not None:
        if nproc_per_node < 1:
            raise ConfigContractError(f"`stages.{stage.name}.launcher.nproc_per_node` must be >= 1")
        if sizing_resolution.resolved_gpus_per_node > 0 and nproc_per_node > sizing_resolution.resolved_gpus_per_node:
            raise ConfigContractError(
                f"`stages.{stage.name}.launcher.nproc_per_node` cannot exceed "
                f"resources.gpus_per_node ({sizing_resolution.resolved_gpus_per_node})"
            )
    rendezvous = stage.launcher.options.get("rendezvous") or {}
    if not isinstance(rendezvous, dict):
        raise ConfigContractError(f"`stages.{stage.name}.launcher.rendezvous` must be a mapping")
    require_port(
        rendezvous.get("port", stage.launcher.options.get("master_port", 29500)),
        field=f"stages.{stage.name}.launcher.rendezvous.port",
    )
