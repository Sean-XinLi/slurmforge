from __future__ import annotations

from ..errors import ConfigContractError
from .models import ExperimentSpec


def validate_topology_contract(spec: ExperimentSpec) -> None:
    unknown_stages = sorted(set(spec.stages) - {"train", "eval"})
    if unknown_stages:
        raise ConfigContractError(f"Unsupported stage keys: {', '.join(unknown_stages)}")
    if "eval" in spec.enabled_stages and spec.enabled_stages["eval"].depends_on and "train" not in spec.enabled_stages:
        raise ConfigContractError("enabled eval depends on train, but train is not enabled")
    for stage in spec.enabled_stages.values():
        if stage.name not in {"train", "eval"}:
            raise ConfigContractError("Stage-batch v1 only supports train and eval")
        if stage.kind != stage.name:
            raise ConfigContractError(f"`stages.{stage.name}.kind` must match the stage key")
        if stage.name == "train" and stage.depends_on:
            raise ConfigContractError("train must not depend on any stage")
        if stage.name == "eval" and stage.depends_on not in {(), ("train",)}:
            raise ConfigContractError("eval may only depend on train")
