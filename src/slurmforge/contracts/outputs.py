from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config_contract.option_sets import OUTPUT_KIND_FILE
from ..config_contract.registry import default_for
from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema
from ..record_fields import (
    required_bool,
    required_object,
    required_record,
    required_string,
    required_string_array,
)
from .output_selectors import normalize_output_selector

DEFAULT_OUTPUT_DISCOVER_SELECT = default_for("stages.*.outputs.*.discover.select")
DEFAULT_OUTPUT_JSON_PATH = default_for("stages.*.outputs.*.json_path")
DEFAULT_OUTPUT_REQUIRED = default_for("stages.*.outputs.*.required")


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


def output_discovery_rule_from_dict(
    payload: dict[str, Any],
    *,
    allow_select: bool = False,
) -> OutputDiscoveryRule | FileOutputDiscoveryRule:
    require_schema(
        payload, name="output_discovery_rule", version=SchemaVersion.OUTPUT_CONTRACT
    )
    globs = required_string_array(payload, "globs", label="output_discovery_rule")
    if "select" in payload:
        if not allow_select:
            raise RecordContractError(
                "output discover select is only supported for file outputs"
            )
        return FileOutputDiscoveryRule(
            globs=globs,
            select=normalize_output_selector(
                required_string(
                    payload, "select", label="output_discovery_rule", non_empty=True
                )
            ),
        )
    if allow_select:
        raise RecordContractError(
            "output_discovery_rule.select is required for file outputs"
        )
    return OutputDiscoveryRule(globs=globs)


def stage_output_spec_from_dict(payload: dict[str, Any]) -> StageOutputSpec:
    require_schema(
        payload, name="stage_output_spec", version=SchemaVersion.OUTPUT_CONTRACT
    )
    kind = required_string(
        payload, "kind", label="stage_output_spec", non_empty=True
    )
    return StageOutputSpec(
        name=required_string(
            payload, "name", label="stage_output_spec", non_empty=True
        ),
        kind=kind,
        required=required_bool(payload, "required", label="stage_output_spec"),
        discover=output_discovery_rule_from_dict(
            required_object(payload, "discover", label="stage_output_spec"),
            allow_select=kind == OUTPUT_KIND_FILE,
        ),
        file=required_string(payload, "file", label="stage_output_spec"),
        json_path=required_string(payload, "json_path", label="stage_output_spec"),
    )


def stage_output_contract_from_dict(
    payload: dict[str, Any] | StageOutputContract,
) -> StageOutputContract:
    if isinstance(payload, StageOutputContract):
        return payload
    values = required_record(payload, "stage_output_contract")
    require_schema(
        values, name="stage_output_contract", version=SchemaVersion.OUTPUT_CONTRACT
    )
    outputs = required_object(values, "outputs", label="stage_output_contract")
    return StageOutputContract(
        outputs={
            _output_name(name): stage_output_spec_from_dict(
                required_record(item, f"stage_output_contract.outputs.{name}")
            )
            for name, item in outputs.items()
        }
    )


def _output_name(name: Any) -> str:
    if not isinstance(name, str) or not name:
        raise RecordContractError(
            "stage_output_contract.outputs keys must be non-empty strings"
        )
    return name
