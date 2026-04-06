from __future__ import annotations

from dataclasses import replace

from ....errors import PlanningError
from ...config.api import EvalConfigSpec
from ...config.normalize import ensure_launcher_config
from ...config.runtime import serialize_launcher_config
from ...utils import deep_merge
from ..contracts import StageExecutionPlan
from ..enums import LauncherKind
from .common import launcher_override_dict


def resolve_eval_launcher(
    *,
    eval_spec: EvalConfigSpec,
    train_stage: StageExecutionPlan,
) -> tuple[object, str, LauncherKind]:
    inherited_mode = "ddp" if train_stage.topology.total_processes > 1 else "single"
    requested_mode = eval_spec.launch_mode or inherited_mode
    if requested_mode == "inherit":
        requested_mode = inherited_mode

    if train_stage.launcher_cfg is None:
        raise PlanningError("train stage is missing resolved launcher_cfg")

    inherited_launcher = deep_merge(
        serialize_launcher_config(train_stage.launcher_cfg),
        {
            "distributed": {
                "nnodes": train_stage.topology.nodes,
                "nproc_per_node": train_stage.topology.processes_per_node,
            }
        },
    )
    eval_launcher = ensure_launcher_config(
        deep_merge(inherited_launcher, launcher_override_dict(eval_spec.launcher))
    )
    launcher_kind = LauncherKind.DDP if requested_mode == "ddp" else LauncherKind.SINGLE
    if launcher_kind == LauncherKind.SINGLE:
        eval_launcher = replace(
            eval_launcher,
            mode="single",
            distributed=replace(eval_launcher.distributed, nnodes=1, nproc_per_node=1),
        )
    else:
        default_nnodes = int(train_stage.topology.nodes)
        default_nproc = int(train_stage.topology.processes_per_node)
        eval_launcher = replace(
            eval_launcher,
            mode="ddp",
            distributed=replace(
                eval_launcher.distributed,
                nnodes=max(1, int(eval_launcher.distributed.nnodes or default_nnodes)),
                nproc_per_node=int(eval_launcher.distributed.nproc_per_node or default_nproc),
            ),
        )
    return eval_launcher, str(requested_mode), launcher_kind
