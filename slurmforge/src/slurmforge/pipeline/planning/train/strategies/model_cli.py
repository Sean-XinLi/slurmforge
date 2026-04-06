from __future__ import annotations

from .....errors import PlanningError
from ...enums import InvocationKind
from ..context import TrainContext
from .base import TrainModeStrategy
from .scripted import build_scripted_stage_plan


class ModelCliTrainStrategy(TrainModeStrategy):
    mode = "model_cli"

    def build(self, ctx: TrainContext):
        if ctx.model_spec.script is None:
            raise PlanningError("model_cli mode requires model_spec.script")
        return build_scripted_stage_plan(
            ctx,
            invocation_kind=InvocationKind.MODEL_CLI,
            launcher_cfg=ctx.launcher_cfg,
            requested_mode=str(ctx.launcher_cfg.mode or "auto"),
            ddp_supported=ctx.model_spec.ddp_supported,
            ddp_required=ctx.model_spec.ddp_required,
            script_path=ctx.model_spec.script,
            cli_args=ctx.run_args,
            explicit_workdir=None,
        )
