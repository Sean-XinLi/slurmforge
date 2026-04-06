from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path

from .....errors import PlanningError
from ....utils import deep_merge
from ....config.normalize import normalize_launcher
from ....config.runtime import serialize_launcher_config
from ...enums import InvocationKind
from ..context import TrainContext
from .base import TrainModeStrategy
from .scripted import build_scripted_stage_plan


class AdapterTrainStrategy(TrainModeStrategy):
    mode = "adapter"

    def build(self, ctx: TrainContext):
        adapter_cfg = ctx.run_spec.adapter
        if adapter_cfg is None:
            raise PlanningError("run.adapter is required in adapter mode")

        adapter_script = Path(adapter_cfg.script)
        if not adapter_script.is_absolute():
            adapter_script = (ctx.project_root / adapter_script).resolve()
        if not adapter_script.exists():
            raise FileNotFoundError(f"Adapter script not found: {adapter_script}")

        adapter_args = copy.deepcopy(adapter_cfg.args)
        resume_from_checkpoint = str(ctx.run_spec.resume_from_checkpoint or "").strip()
        if resume_from_checkpoint:
            adapter_args.setdefault("resume_from_checkpoint", resume_from_checkpoint)
        if adapter_cfg.pass_run_args:
            adapter_args[adapter_cfg.run_args_flag] = ctx.run_args
        if ctx.model_overrides and adapter_cfg.pass_model_overrides:
            adapter_args[adapter_cfg.model_overrides_flag] = ctx.model_overrides

        adapter_launcher = normalize_launcher(
            deep_merge(
                serialize_launcher_config(ctx.launcher_cfg),
                serialize_launcher_config(adapter_cfg.launcher),
            )
        )
        if adapter_cfg.launch_mode is not None:
            adapter_launcher = replace(adapter_launcher, mode=adapter_cfg.launch_mode)

        ddp_supported = ctx.model_spec.ddp_supported if adapter_cfg.ddp_supported is None else adapter_cfg.ddp_supported
        ddp_required = bool(adapter_cfg.ddp_required)
        return build_scripted_stage_plan(
            ctx,
            invocation_kind=InvocationKind.ADAPTER,
            launcher_cfg=adapter_launcher,
            requested_mode=str(adapter_launcher.mode or "auto"),
            ddp_supported=bool(ddp_supported),
            ddp_required=ddp_required,
            script_path=adapter_script,
            cli_args=adapter_args,
            explicit_workdir=adapter_cfg.workdir,
        )
