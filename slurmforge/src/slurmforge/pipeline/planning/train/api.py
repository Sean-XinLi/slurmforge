from __future__ import annotations

from ....errors import PlanningError
from ....model_support.gpu_estimator import GpuEstimate, build_estimator, load_model_defaults
from ..validator import validate_stage_execution_plan
from ..contracts import StageExecutionPlan
from .context import PreparedTrainPlan, TrainContext
from .strategies.adapter import AdapterTrainStrategy
from .strategies.base import TrainModeStrategy
from .strategies.command import CommandTrainStrategy
from .strategies.model_cli import ModelCliTrainStrategy


def get_train_strategy(mode: str) -> TrainModeStrategy:
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode == "command":
        return CommandTrainStrategy()
    if normalized_mode == "adapter":
        return AdapterTrainStrategy()
    if normalized_mode == "model_cli":
        return ModelCliTrainStrategy()
    raise PlanningError(f"Unsupported train mode: {mode}")


def build_train_resolution(prepared: PreparedTrainPlan, *, run_index: int) -> StageExecutionPlan:
    model_defaults = load_model_defaults(prepared.model_spec)
    estimator = build_estimator(str(prepared.resources_cfg.gpu_estimator))
    estimate: GpuEstimate = estimator.estimate(
        model_spec=prepared.model_spec,
        run_args=prepared.run_args,
        model_overrides=prepared.model_overrides,
        model_defaults=model_defaults,
        resources_cfg=prepared.resources_cfg,
    )

    strategy = get_train_strategy(prepared.train_mode)
    stage_plan = strategy.build(
        TrainContext(
            run_index=run_index,
            project_root=prepared.project_root,
            run_spec=prepared.spec.run,
            model_spec=prepared.model_spec,
            launcher_cfg=prepared.launcher_cfg,
            cluster_cfg=prepared.cluster_cfg,
            launcher_nproc_per_node_explicit=prepared.launcher_nproc_per_node_explicit,
            cluster_nodes_explicit=prepared.cluster_nodes_explicit,
            cluster_gpus_per_node_explicit=prepared.cluster_gpus_per_node_explicit,
            resources_cfg=prepared.resources_cfg,
            run_args=prepared.run_args,
            model_overrides=prepared.model_overrides,
            estimate=estimate,
            validation_cfg=prepared.validation_cfg,
        )
    )
    return validate_stage_execution_plan(stage_plan, prepared.validation_cfg)
