from __future__ import annotations

from ...plans.launcher import LauncherPlan, RendezvousPlan
from ...plans.resources import ResourcePlan
from ...spec import StageSpec


def launcher_payload(stage: StageSpec, resources: ResourcePlan) -> LauncherPlan:
    launcher_type = stage.launcher.type
    if launcher_type == "torchrun":
        return _torchrun_launcher_payload(stage, resources)
    if launcher_type in {"srun", "mpirun"}:
        return LauncherPlan(type=launcher_type, args=stage.launcher.args)
    return LauncherPlan(type=launcher_type)


def _torchrun_launcher_payload(stage: StageSpec, resources: ResourcePlan) -> LauncherPlan:
    nodes = resources.nodes
    gpus = resources.gpus_per_node
    nproc_default = gpus if gpus > 0 else 1
    torchrun = stage.launcher.torchrun
    mode = torchrun.mode or ("multi_node" if nodes > 1 else "single_node")
    rendezvous = torchrun.rendezvous
    return LauncherPlan(
        type="torchrun",
        mode=mode,
        nnodes=_auto_int(torchrun.nnodes, nodes),
        nproc_per_node=_auto_int(
            torchrun.nproc_per_node,
            nproc_default,
        ),
        rendezvous=RendezvousPlan(
            backend=rendezvous.backend,
            endpoint=rendezvous.endpoint,
            port=_auto_int(rendezvous.port, 29500),
        ),
        srun_args=torchrun.srun_args,
    )


def _auto_int(value: int | str | None, default: int) -> int:
    if value in (None, "", "auto"):
        return int(default)
    return int(value)
