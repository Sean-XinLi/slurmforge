from __future__ import annotations

from typing import Any

from ..launcher import BeforeStepPlan, LauncherPlan, RendezvousPlan


def before_step_plan_from_dict(payload: dict[str, Any]) -> BeforeStepPlan:
    return BeforeStepPlan(run=str(payload["run"]), name=str(payload["name"]))


def launcher_plan_from_dict(payload: dict[str, Any]) -> LauncherPlan:
    rendezvous_raw = payload["rendezvous"]
    return LauncherPlan(
        type=str(payload["type"]),
        mode=str(payload["mode"]),
        nnodes=None if payload["nnodes"] in (None, "") else int(payload["nnodes"]),
        nproc_per_node=None
        if payload["nproc_per_node"] in (None, "")
        else int(payload["nproc_per_node"]),
        rendezvous=None
        if rendezvous_raw is None
        else RendezvousPlan(
            backend=str(rendezvous_raw["backend"]),
            endpoint=str(rendezvous_raw["endpoint"]),
            port=int(rendezvous_raw["port"]),
        ),
        args=tuple(str(item) for item in payload["args"]),
        srun_args=tuple(str(item) for item in payload["srun_args"]),
    )
