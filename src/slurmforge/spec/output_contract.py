from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..errors import ConfigContractError
from ..io import SchemaVersion


OUTPUT_KINDS = {"file", "files", "metric", "manifest"}
OUTPUT_SELECTORS = {"latest_step", "first", "last"}


@dataclass(frozen=True)
class OutputDiscoveryRule:
    globs: tuple[str, ...] = ()
    select: str = "latest_step"
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


@dataclass(frozen=True)
class StageOutputSpec:
    name: str
    kind: str
    required: bool = False
    discover: OutputDiscoveryRule = field(default_factory=OutputDiscoveryRule)
    file: str = ""
    json_path: str = "$"
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


@dataclass(frozen=True)
class StageOutputContract:
    outputs: dict[str, StageOutputSpec] = field(default_factory=dict)
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


def _require_schema(payload: dict[str, Any], *, name: str, require_present: bool = False) -> None:
    if require_present and "schema_version" not in payload:
        raise ConfigContractError(f"`{name}.schema_version` is required")
    version = int(payload.get("schema_version", SchemaVersion.OUTPUT_CONTRACT))
    if version != SchemaVersion.OUTPUT_CONTRACT:
        raise ConfigContractError(f"`{name}.schema_version` is not supported: {version}")


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigContractError(f"`{name}` must be a mapping")
    return value


def _string_tuple(value: Any, *, name: str) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        items = (value,)
    elif isinstance(value, (list, tuple)):
        items = tuple(value)
    else:
        raise ConfigContractError(f"`{name}` must be a string or list of strings")
    if not all(isinstance(item, str) and item for item in items):
        raise ConfigContractError(f"`{name}` must contain non-empty strings")
    return tuple(str(item) for item in items)


def _normalize_selector(value: Any) -> str:
    selector = str(value or "latest_step")
    if selector == "latest":
        selector = "latest_step"
    if selector not in OUTPUT_SELECTORS:
        raise ConfigContractError("output discover select must be latest_step, latest, first, or last")
    return selector


def _parse_discovery(raw: Any, *, name: str) -> OutputDiscoveryRule:
    data = {} if raw in (None, "") else _require_mapping(raw, name)
    _require_schema(data, name=name)
    return OutputDiscoveryRule(
        globs=_string_tuple(data.get("globs"), name=f"{name}.globs"),
        select=_normalize_selector(data.get("select")),
    )


def _parse_output_spec(output_name: str, raw: Any, *, stage_name: str) -> StageOutputSpec:
    name = f"stages.{stage_name}.outputs.{output_name}"
    data = _require_mapping(raw, name)
    _require_schema(data, name=name)
    kind = str(data.get("kind") or "")
    if kind not in OUTPUT_KINDS:
        raise ConfigContractError(f"`{name}.kind` must be file, files, metric, or manifest")
    discover = _parse_discovery(data.get("discover"), name=f"{name}.discover")
    file_value = data.get("file")
    json_path = str(data.get("json_path") or "$")
    return StageOutputSpec(
        name=output_name,
        kind=kind,
        required=bool(data.get("required", False)),
        discover=discover,
        file="" if file_value in (None, "") else str(file_value),
        json_path=json_path,
    )


def parse_stage_output_contract(raw: Any, *, stage_name: str) -> StageOutputContract:
    if raw in (None, ""):
        return StageOutputContract()
    data = _require_mapping(raw, f"stages.{stage_name}.outputs")
    _require_schema(data, name=f"stages.{stage_name}.outputs")
    outputs = {
        str(output_name): _parse_output_spec(str(output_name), output_raw, stage_name=stage_name)
        for output_name, output_raw in sorted(data.items())
        if output_name != "schema_version"
    }
    return StageOutputContract(outputs=outputs)


def output_discovery_rule_from_dict(payload: dict[str, Any]) -> OutputDiscoveryRule:
    _require_schema(payload, name="output_discovery_rule", require_present=True)
    return OutputDiscoveryRule(
        globs=tuple(str(item) for item in payload.get("globs", ())),
        select=_normalize_selector(payload.get("select")),
    )


def stage_output_spec_from_dict(payload: dict[str, Any]) -> StageOutputSpec:
    _require_schema(payload, name="stage_output_spec", require_present=True)
    return StageOutputSpec(
        name=str(payload["name"]),
        kind=str(payload["kind"]),
        required=bool(payload.get("required", False)),
        discover=output_discovery_rule_from_dict(dict(payload.get("discover") or {})),
        file=str(payload.get("file") or ""),
        json_path=str(payload.get("json_path") or "$"),
    )


def stage_output_contract_from_dict(payload: dict[str, Any] | StageOutputContract | None) -> StageOutputContract:
    if isinstance(payload, StageOutputContract):
        return payload
    if payload is None:
        return StageOutputContract()
    _require_schema(payload, name="stage_output_contract", require_present=True)
    return StageOutputContract(
        outputs={
            str(name): stage_output_spec_from_dict(dict(item))
            for name, item in dict(payload.get("outputs") or {}).items()
        }
    )
