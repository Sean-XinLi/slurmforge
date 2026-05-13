from __future__ import annotations

from ..config_contract.option_sets import (
    ENTRY_COMMAND,
    ENTRY_PYTHON_SCRIPT,
    LAUNCHER_COMMAND,
    LAUNCHER_MODE_MULTI_NODE,
    LAUNCHER_MODE_SINGLE_NODE,
    LAUNCHER_MPIRUN,
    LAUNCHER_SINGLE,
    LAUNCHER_SRUN,
    LAUNCHER_TORCHRUN,
)
from ..config_contract.registry import default_for, options_for, options_sentence
from ..errors import ConfigContractError
from ..sizing.models import GpuSizingResolution
from .models import StageSpec
from .validation_common import explicit_int, require_port


def validate_launcher_contract(
    stage: StageSpec, *, sizing_resolution: GpuSizingResolution
) -> None:
    if stage.launcher.type not in options_for("stages.*.launcher.type"):
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type` must be "
            f"{options_sentence('stages.*.launcher.type')}"
        )
    if stage.launcher.type == LAUNCHER_TORCHRUN:
        _validate_torchrun_launcher(stage, sizing_resolution=sizing_resolution)
    if stage.entry.type == ENTRY_COMMAND and stage.launcher.type not in {
        LAUNCHER_SINGLE,
        LAUNCHER_COMMAND,
        LAUNCHER_SRUN,
        LAUNCHER_MPIRUN,
    }:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type={stage.launcher.type}` cannot wrap command entries"
        )


def _validate_torchrun_launcher(
    stage: StageSpec, *, sizing_resolution: GpuSizingResolution
) -> None:
    if stage.entry.type != ENTRY_PYTHON_SCRIPT:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type=torchrun` requires a python_script entry"
        )
    torchrun = stage.launcher.torchrun
    mode = str(
        torchrun.mode
        or (
            LAUNCHER_MODE_MULTI_NODE
            if stage.resources.nodes > 1
            else default_for("stages.*.launcher.mode")
        )
    )
    if mode not in options_for("stages.*.launcher.mode"):
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.mode` must be "
            f"{options_sentence('stages.*.launcher.mode')}"
        )
    if mode == LAUNCHER_MODE_SINGLE_NODE and stage.resources.nodes != 1:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.mode=single_node` requires resources.nodes == 1"
        )
    if mode == LAUNCHER_MODE_MULTI_NODE and stage.resources.nodes < 2:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.mode=multi_node` requires resources.nodes >= 2"
        )
    nnodes = explicit_int(
        torchrun.nnodes,
        field=f"stages.{stage.name}.launcher.nnodes",
    )
    if nnodes is not None and nnodes != stage.resources.nodes:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.nnodes` must equal resources.nodes ({stage.resources.nodes})"
        )
    nproc_per_node = explicit_int(
        torchrun.nproc_per_node,
        field=f"stages.{stage.name}.launcher.nproc_per_node",
    )
    if nproc_per_node is not None:
        if nproc_per_node < 1:
            raise ConfigContractError(
                f"`stages.{stage.name}.launcher.nproc_per_node` must be >= 1"
            )
        if (
            sizing_resolution.resolved_gpus_per_node > 0
            and nproc_per_node > sizing_resolution.resolved_gpus_per_node
        ):
            raise ConfigContractError(
                f"`stages.{stage.name}.launcher.nproc_per_node` cannot exceed "
                f"resources.gpus_per_node ({sizing_resolution.resolved_gpus_per_node})"
            )
    rendezvous = torchrun.rendezvous
    if not rendezvous.backend:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.rendezvous.backend` must be non-empty"
        )
    require_port(
        rendezvous.port,
        field=f"stages.{stage.name}.launcher.rendezvous.port",
    )
