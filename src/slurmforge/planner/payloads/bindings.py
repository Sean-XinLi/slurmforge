from __future__ import annotations

from ...contracts import InputBinding, RunDefinition
from ...resolver.defaults import default_stage_input_bindings
from ...spec import ExperimentSpec, StageSpec


def default_bindings(
    spec: ExperimentSpec, run: RunDefinition, stage: StageSpec
) -> tuple[InputBinding, ...]:
    return default_stage_input_bindings(spec, run, stage)
