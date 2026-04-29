from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config_contract.defaults import (
    DEFAULT_OUTPUT_DISCOVER_SELECT,
    DEFAULT_OUTPUT_JSON_PATH,
    DEFAULT_OUTPUT_REQUIRED,
)
from ..config_contract.options import OUTPUT_KIND_FILE, options_for, options_sentence
from ..errors import ConfigContractError
from ..io import SchemaVersion

OUTPUT_SELECTORS = set(options_for("stages.*.outputs.*.discover.select"))


@dataclass(frozen=True)
class OutputDiscoveryRule:
    globs: tuple[str, ...] = ()
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


@dataclass(frozen=True)
class FileOutputDiscoveryRule:
    globs: tuple[str, ...] = ()
    select: str = DEFAULT_OUTPUT_DISCOVER_SELECT
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


@dataclass(frozen=True)
class StageOutputSpec:
    name: str
    kind: str
    required: bool = DEFAULT_OUTPUT_REQUIRED
    discover: OutputDiscoveryRule | FileOutputDiscoveryRule = field(
        default_factory=OutputDiscoveryRule
    )
    file: str = ""
    json_path: str = DEFAULT_OUTPUT_JSON_PATH
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


@dataclass(frozen=True)
class StageOutputContract:
    outputs: dict[str, StageOutputSpec] = field(default_factory=dict)
    schema_version: int = SchemaVersion.OUTPUT_CONTRACT


def _require_schema(
    payload: dict[str, Any], *, name: str, require_present: bool = False
) -> None:
    if require_present and "schema_version" not in payload:
        raise ConfigContractError(f"`{name}.schema_version` is required")
    version = int(payload.get("schema_version", SchemaVersion.OUTPUT_CONTRACT))
    if version != SchemaVersion.OUTPUT_CONTRACT:
        raise ConfigContractError(
            f"`{name}.schema_version` is not supported: {version}"
        )


def _normalize_selector(value: Any) -> str:
    selector = str(value or DEFAULT_OUTPUT_DISCOVER_SELECT)
    if selector not in OUTPUT_SELECTORS:
        raise ConfigContractError(
            "output discover select must be "
            f"{options_sentence('stages.*.outputs.*.discover.select')}"
        )
    return selector


def output_discovery_rule_from_dict(
    payload: dict[str, Any],
    *,
    allow_select: bool = False,
) -> OutputDiscoveryRule | FileOutputDiscoveryRule:
    _require_schema(payload, name="output_discovery_rule", require_present=True)
    globs = tuple(str(item) for item in payload.get("globs", ()))
    if "select" in payload:
        if not allow_select:
            raise ConfigContractError(
                "output discover select is only supported for file outputs"
            )
        return FileOutputDiscoveryRule(
            globs=globs,
            select=_normalize_selector(payload.get("select")),
        )
    if allow_select:
        return FileOutputDiscoveryRule(globs=globs)
    return OutputDiscoveryRule(globs=globs)


def stage_output_spec_from_dict(payload: dict[str, Any]) -> StageOutputSpec:
    _require_schema(payload, name="stage_output_spec", require_present=True)
    kind = str(payload["kind"])
    return StageOutputSpec(
        name=str(payload["name"]),
        kind=kind,
        required=bool(payload.get("required", DEFAULT_OUTPUT_REQUIRED)),
        discover=output_discovery_rule_from_dict(
            dict(payload.get("discover") or {}),
            allow_select=kind == OUTPUT_KIND_FILE,
        ),
        file=str(payload.get("file") or ""),
        json_path=str(payload.get("json_path") or DEFAULT_OUTPUT_JSON_PATH),
    )


def stage_output_contract_from_dict(
    payload: dict[str, Any] | StageOutputContract | None,
) -> StageOutputContract:
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
