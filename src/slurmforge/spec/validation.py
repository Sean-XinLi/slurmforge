from __future__ import annotations

from ..config_contract.registry import options_for, options_sentence
from ..errors import ConfigContractError
from .models import ExperimentSpec
from .validation_notifications import validate_notifications_contract
from .validation_resources import validate_hardware_contract, validate_sizing_contract
from .validation_runs import validate_runs_contract
from .validation_runtime import validate_runtime_contract
from .validation_stage import validate_stage_contract
from .validation_topology import validate_topology_contract


def validate_experiment_spec(spec: ExperimentSpec, *, check_paths: bool = True) -> None:
    validate_topology_contract(spec)
    validate_hardware_contract(spec)
    validate_sizing_contract(spec)
    validate_runs_contract(spec)
    validate_notifications_contract(spec)
    if spec.dispatch.max_available_gpus < 0:
        raise ConfigContractError("`dispatch.max_available_gpus` must be >= 0")
    validate_runtime_contract(spec)
    _validate_artifact_store_contract(spec)
    for stage in spec.enabled_stages.values():
        validate_stage_contract(spec, stage, check_paths=check_paths)


def _validate_artifact_store_contract(spec: ExperimentSpec) -> None:
    allowed = set(options_for("artifact_store.strategy"))
    if spec.artifact_store.strategy not in allowed:
        raise ConfigContractError(
            f"`artifact_store.strategy` must be {options_sentence('artifact_store.strategy')}"
        )
    if (
        spec.artifact_store.fallback_strategy is not None
        and spec.artifact_store.fallback_strategy not in allowed
    ):
        raise ConfigContractError(
            "`artifact_store.fallback_strategy` must be "
            f"{options_sentence('artifact_store.strategy')}"
        )
