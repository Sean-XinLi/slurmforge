from __future__ import annotations

from dataclasses import dataclass

from ...models import (
    EvalConfigSpec,
    ModelConfigSpec,
    OutputConfigSpec,
    PlanningHints,
    RunConfigSpec,
)
from ...runtime import (
    ArtifactsConfig,
    ClusterConfig,
    EnvConfig,
    LauncherConfig,
    NotifyConfig,
    ResourcesConfig,
    ValidationConfig,
)
from .inputs import ExperimentSectionInputs
from .sections import NormalizedExperimentSections


@dataclass(frozen=True)
class NormalizedExperimentContract:
    project: str
    experiment_name: str
    model: ModelConfigSpec | None
    run: RunConfigSpec
    launcher: LauncherConfig
    cluster: ClusterConfig
    env: EnvConfig
    resources: ResourcesConfig
    artifacts: ArtifactsConfig
    eval: EvalConfigSpec
    output: OutputConfigSpec
    notify: NotifyConfig
    validation: ValidationConfig
    hints: PlanningHints


def assemble_experiment_contract(
    inputs: ExperimentSectionInputs,
    sections: NormalizedExperimentSections,
    *,
    hints: PlanningHints,
) -> NormalizedExperimentContract:
    return NormalizedExperimentContract(
        project=inputs.project,
        experiment_name=inputs.experiment_name,
        model=sections.model,
        run=sections.run,
        launcher=sections.launcher,
        cluster=sections.cluster,
        env=sections.env,
        resources=sections.resources,
        artifacts=sections.artifacts,
        eval=sections.eval,
        output=sections.output,
        notify=sections.notify,
        validation=sections.validation,
        hints=hints,
    )
