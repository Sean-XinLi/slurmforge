from __future__ import annotations

from ..errors import ConfigContractError
from .models import ExperimentSpec


def stage_name_for_kind(spec: ExperimentSpec, kind: str) -> str:
    matches = [
        name for name, stage in spec.enabled_stages.items() if stage.kind == kind
    ]
    if not matches:
        raise ConfigContractError(f"No enabled `{kind}` stage exists in this spec")
    if kind in matches:
        return kind
    if len(matches) > 1:
        joined = ", ".join(sorted(matches))
        raise ConfigContractError(
            f"Multiple enabled `{kind}` stages exist ({joined}); address a stage by name"
        )
    return matches[0]


def stage_source_input_name(spec: ExperimentSpec, *, stage_name: str) -> str:
    stage = spec.enabled_stages[stage_name]
    if not stage.inputs:
        raise ConfigContractError(
            f"`stages.{stage_name}.inputs` must declare at least one input"
        )
    required = [
        name for name, input_spec in sorted(stage.inputs.items()) if input_spec.required
    ]
    if len(required) == 1:
        return required[0]
    if len(stage.inputs) == 1:
        return next(iter(stage.inputs))
    raise ConfigContractError(
        f"`stages.{stage_name}` has multiple inputs; pass --input-name explicitly"
    )
