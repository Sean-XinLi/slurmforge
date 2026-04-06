from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from ...config.codecs import ensure_eval_train_outputs_config
from ...config.models import EvalTrainOutputsConfig
from ...config.normalize import ensure_artifacts_config, ensure_cluster_config, ensure_env_config
from ...config.runtime import ArtifactsConfig, ClusterConfig, EnvConfig
from ...config.utils import ensure_dict
from ...planning.contracts import (
    PlanDiagnostic,
    StageExecutionPlan,
    ensure_plan_diagnostic,
    ensure_stage_execution_plan,
)
from .dispatch import DispatchInfo
from .metadata import GeneratedBy
from ..codecs.metadata import ensure_generated_by


@dataclass(frozen=True)
class RunPlan:
    run_index: int
    total_runs: int
    run_id: str
    project: str
    experiment_name: str
    model_name: str
    train_mode: str
    train_stage: StageExecutionPlan
    eval_stage: StageExecutionPlan | None
    cluster: ClusterConfig
    env: EnvConfig
    run_dir: str
    dispatch: DispatchInfo
    artifacts: ArtifactsConfig
    eval_train_outputs: EvalTrainOutputsConfig = field(default_factory=lambda: ensure_eval_train_outputs_config({}))
    sweep_case_name: str | None = None
    sweep_assignments: dict[str, Any] = field(default_factory=dict)
    generated_by: GeneratedBy = field(default_factory=GeneratedBy)
    run_dir_rel: str | None = None
    planning_diagnostics: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_index", int(self.run_index))
        object.__setattr__(self, "total_runs", int(self.total_runs))
        object.__setattr__(self, "run_id", str(self.run_id))
        object.__setattr__(self, "project", str(self.project))
        object.__setattr__(self, "experiment_name", str(self.experiment_name))
        object.__setattr__(self, "model_name", str(self.model_name))
        object.__setattr__(self, "train_mode", str(self.train_mode))
        object.__setattr__(self, "train_stage", ensure_stage_execution_plan(self.train_stage, name="train_stage"))
        object.__setattr__(
            self,
            "eval_stage",
            None if self.eval_stage is None else ensure_stage_execution_plan(self.eval_stage, name="eval_stage"),
        )
        object.__setattr__(self, "cluster", ensure_cluster_config(self.cluster))
        object.__setattr__(self, "env", ensure_env_config(self.env))
        object.__setattr__(self, "run_dir", str(self.run_dir or ""))
        object.__setattr__(
            self,
            "dispatch",
            self.dispatch if isinstance(self.dispatch, DispatchInfo) else DispatchInfo(**ensure_dict(self.dispatch, "dispatch")),
        )
        object.__setattr__(self, "artifacts", ensure_artifacts_config(self.artifacts))
        object.__setattr__(
            self,
            "eval_train_outputs",
            ensure_eval_train_outputs_config(self.eval_train_outputs, name="eval_train_outputs"),
        )
        object.__setattr__(self, "sweep_case_name", None if self.sweep_case_name in (None, "") else str(self.sweep_case_name))
        object.__setattr__(self, "sweep_assignments", copy.deepcopy(dict(self.sweep_assignments or {})))
        object.__setattr__(self, "generated_by", ensure_generated_by(self.generated_by))
        object.__setattr__(self, "run_dir_rel", None if self.run_dir_rel in (None, "") else str(self.run_dir_rel))
        object.__setattr__(
            self,
            "planning_diagnostics",
            tuple(ensure_plan_diagnostic(item, name="planning_diagnostics[]") for item in self.planning_diagnostics),
        )


RUN_RECORD_EXECUTION_FIELDS = (
    "run_index",
    "total_runs",
    "run_id",
    "generated_by",
    "project",
    "experiment_name",
    "model_name",
    "train_mode",
    "train_stage",
    "eval_stage",
    "eval_train_outputs",
    "run_dir_rel",
    "cluster",
    "env",
    "artifacts",
    "sweep_case_name",
    "sweep_assignments",
    "dispatch",
)

RUN_RECORD_OBSERVABILITY_FIELDS = (
    "planning_diagnostics",
)

RUN_RECORD_TOP_LEVEL_FIELDS = (
    *RUN_RECORD_EXECUTION_FIELDS,
    *RUN_RECORD_OBSERVABILITY_FIELDS,
)
