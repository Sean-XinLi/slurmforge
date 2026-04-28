from __future__ import annotations

import copy
from typing import Any

from ...plans.launcher import LauncherPlan, RendezvousPlan
from ...plans.resources import ResourcePlan
from ...spec import StageSpec


def launcher_payload(stage: StageSpec, resources: ResourcePlan) -> LauncherPlan:
    launcher_type = stage.launcher.type
    options = copy.deepcopy(stage.launcher.options)
    if launcher_type == "torchrun":
        return _torchrun_launcher_payload(stage, resources, options)
    if launcher_type in {"srun", "mpirun"}:
        args = options.get("args") or ()
        return LauncherPlan(type=launcher_type, args=tuple(str(item) for item in args))
    return LauncherPlan(type=launcher_type)


def _torchrun_launcher_payload(stage: StageSpec, resources: ResourcePlan, options: dict[str, Any]) -> LauncherPlan:
    nodes = resources.nodes
    gpus = resources.gpus_per_node
    nproc_default = gpus if gpus > 0 else 1
    mode = str(options.get("mode") or ("multi_node" if nodes > 1 else "single_node"))
    rendezvous = dict(options.get("rendezvous") or {})
    port = rendezvous.get("port", options.get("master_port", 29500))
    master_port = None if options.get("master_port") is None else int(options["master_port"])
    return LauncherPlan(
        type="torchrun",
        mode=mode,
        nnodes=_auto_int(options.get("nnodes") or options.get("nodes") or "auto", nodes),
        nproc_per_node=_auto_int(
            options.get("nproc_per_node") or options.get("processes_per_node") or "auto",
            nproc_default,
        ),
        rendezvous=RendezvousPlan(
            backend=str(rendezvous.get("backend") or "c10d"),
            endpoint=str(rendezvous.get("endpoint") or "auto"),
            port=int(port),
        ),
        srun_args=tuple(str(item) for item in options.get("srun_args") or ()),
        master_port=master_port,
    )


def _auto_int(value: Any, default: int) -> int:
    if value in (None, "", "auto"):
        return int(default)
    return int(value)
