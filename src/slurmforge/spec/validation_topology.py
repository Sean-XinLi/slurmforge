from __future__ import annotations

from ..config_contract.workflows import STAGE_EVAL, STAGE_TRAIN, SUPPORTED_STAGE_KEYS
from ..errors import ConfigContractError
from .models import ExperimentSpec


def validate_topology_contract(spec: ExperimentSpec) -> None:
    unknown_stages = sorted(set(spec.stages) - SUPPORTED_STAGE_KEYS)
    if unknown_stages:
        raise ConfigContractError(
            f"Unsupported stage keys: {', '.join(unknown_stages)}"
        )
    if (
        STAGE_EVAL in spec.enabled_stages
        and spec.enabled_stages[STAGE_EVAL].depends_on
        and STAGE_TRAIN not in spec.enabled_stages
    ):
        raise ConfigContractError(
            "enabled eval depends on train, but train is not enabled"
        )
    for stage in spec.enabled_stages.values():
        if stage.name not in SUPPORTED_STAGE_KEYS:
            raise ConfigContractError("Unsupported stage key")
        if stage.kind != stage.name:
            raise ConfigContractError(
                f"`stages.{stage.name}.kind` must match the stage key"
            )
        if stage.name == STAGE_TRAIN and stage.depends_on:
            raise ConfigContractError("train must not depend on any stage")
        if stage.name == STAGE_EVAL and stage.depends_on not in {(), (STAGE_TRAIN,)}:
            raise ConfigContractError("eval may only depend on train")
