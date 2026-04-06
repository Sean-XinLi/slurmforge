from __future__ import annotations

from typing import Any, Callable

from ....errors import ConfigContractError
from ..constants import REPLAY_MODEL_CATALOG_KEY
from ..utils import ensure_dict
from .definitions import (
    ADAPTER_SCHEMA,
    ARTIFACTS_SCHEMA,
    CLUSTER_SCHEMA,
    DISTRIBUTED_SCHEMA,
    ENV_SCHEMA,
    EVAL_SCHEMA,
    EVAL_TRAIN_OUTPUTS_SCHEMA,
    EXTERNAL_RUNTIME_SCHEMA,
    LAUNCHER_SCHEMA,
    MODEL_CATALOG_SCHEMA,
    MODEL_REGISTRY_SCHEMA,
    MODEL_SCHEMA,
    NOTIFY_SCHEMA,
    OUTPUT_SCHEMA,
    RESOURCES_SCHEMA,
    RUN_SCHEMA,
    VALIDATION_SCHEMA,
)
from .traversal import validate_mapping_schema


def validate_model_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=MODEL_SCHEMA)


def validate_unique_model_names(entries: list[dict[str, Any]], *, name: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for entry in entries:
        model_name = str(entry.get("name", "")).strip()
        if not model_name:
            continue
        if model_name in seen:
            duplicates.add(model_name)
        else:
            seen.add(model_name)
    if duplicates:
        joined = ", ".join(sorted(duplicates))
        raise ConfigContractError(f"{name} defines duplicate model names: {joined}")


def validate_launcher_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=LAUNCHER_SCHEMA)
    distributed = ensure_dict(data.get("distributed"), f"{name}.distributed")
    validate_mapping_schema(distributed, name=f"{name}.distributed", schema=DISTRIBUTED_SCHEMA)
    if "ddp" in data:
        ddp_cfg = ensure_dict(data.get("ddp"), f"{name}.ddp")
        validate_mapping_schema(ddp_cfg, name=f"{name}.ddp", schema=DISTRIBUTED_SCHEMA)


def validate_model_registry_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=MODEL_REGISTRY_SCHEMA)
    registry_file = data.get("registry_file")
    if registry_file not in (None, "") and not isinstance(registry_file, str):
        raise ConfigContractError(f"{name}.registry_file must be a string when provided")
    raw_entries = data.get("extra_models")
    if raw_entries in (None, ""):
        return
    if not isinstance(raw_entries, list):
        raise ConfigContractError(f"{name}.extra_models must be a list when provided")
    for idx, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise ConfigContractError(f"{name}.extra_models[{idx}] must be a mapping")
        validate_model_schema(entry, name=f"{name}.extra_models[{idx}]")
    validate_unique_model_names(raw_entries, name=f"{name}.extra_models")


def validate_model_catalog_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=MODEL_CATALOG_SCHEMA)
    raw_entries = data.get("models")
    if raw_entries in (None, ""):
        return
    if not isinstance(raw_entries, list):
        raise ConfigContractError(f"{name}.models must be a list when provided")
    for idx, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise ConfigContractError(f"{name}.models[{idx}] must be a mapping")
        validate_model_schema(entry, name=f"{name}.models[{idx}]")
    validate_unique_model_names(raw_entries, name=f"{name}.models")


def validate_run_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=RUN_SCHEMA)
    adapter_cfg = ensure_dict(data.get("adapter"), f"{name}.adapter")
    if adapter_cfg:
        validate_mapping_schema(adapter_cfg, name=f"{name}.adapter", schema=ADAPTER_SCHEMA)
        validate_launcher_schema(
            ensure_dict(adapter_cfg.get("launcher"), f"{name}.adapter.launcher"),
            name=f"{name}.adapter.launcher",
        )
    external_runtime_cfg = ensure_dict(data.get("external_runtime"), f"{name}.external_runtime")
    if external_runtime_cfg:
        validate_mapping_schema(external_runtime_cfg, name=f"{name}.external_runtime", schema=EXTERNAL_RUNTIME_SCHEMA)


def validate_eval_schema(data: dict[str, Any], *, name: str) -> None:
    validate_mapping_schema(data, name=name, schema=EVAL_SCHEMA)
    validate_launcher_schema(
        ensure_dict(data.get("launcher"), f"{name}.launcher"),
        name=f"{name}.launcher",
    )
    external_runtime_cfg = ensure_dict(data.get("external_runtime"), f"{name}.external_runtime")
    if external_runtime_cfg:
        validate_mapping_schema(external_runtime_cfg, name=f"{name}.external_runtime", schema=EXTERNAL_RUNTIME_SCHEMA)
    train_outputs_cfg = ensure_dict(data.get("train_outputs"), f"{name}.train_outputs")
    if train_outputs_cfg:
        validate_mapping_schema(train_outputs_cfg, name=f"{name}.train_outputs", schema=EVAL_TRAIN_OUTPUTS_SCHEMA)


SECTION_VALIDATORS: dict[str, Callable[[dict[str, Any]], None] | Callable[..., None]] = {
    "model": validate_model_schema,
    "model_registry": validate_model_registry_schema,
    REPLAY_MODEL_CATALOG_KEY: validate_model_catalog_schema,
    "run": validate_run_schema,
    "launcher": validate_launcher_schema,
    "cluster": lambda data, *, name: validate_mapping_schema(data, name=name, schema=CLUSTER_SCHEMA),
    "env": lambda data, *, name: validate_mapping_schema(data, name=name, schema=ENV_SCHEMA),
    "resources": lambda data, *, name: validate_mapping_schema(data, name=name, schema=RESOURCES_SCHEMA),
    "artifacts": lambda data, *, name: validate_mapping_schema(data, name=name, schema=ARTIFACTS_SCHEMA),
    "eval": validate_eval_schema,
    "output": lambda data, *, name: validate_mapping_schema(data, name=name, schema=OUTPUT_SCHEMA),
    "notify": lambda data, *, name: validate_mapping_schema(data, name=name, schema=NOTIFY_SCHEMA),
    "validation": lambda data, *, name: validate_mapping_schema(data, name=name, schema=VALIDATION_SCHEMA),
}
