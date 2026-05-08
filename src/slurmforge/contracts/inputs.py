"""Input contract dataclasses shared by spec, planner, resolver, and executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config_contract.option_sets import (
    INPUT_EXPECTS_MANIFEST,
    INPUT_EXPECTS_PATH,
    INPUT_EXPECTS_VALUE,
    INPUT_INJECT_JSON,
    INPUT_INJECT_PATH,
    INPUT_INJECT_VALUE,
    OUTPUT_KIND_FILES,
    OUTPUT_KIND_MANIFEST,
    OUTPUT_KIND_METRIC,
)
from ..config_contract.registry import default_for
from ..io import SchemaVersion, require_schema, stable_json
from ..record_fields import (
    required_json_value,
    required_object,
    required_record,
    required_string,
)

JsonObject = dict[str, Any]
DEFAULT_INPUT_INJECT_MODE = default_for("stages.*.inputs.*.inject.mode")


@dataclass(frozen=True)
class InputInjection:
    flag: str | None = None
    env: str | None = None
    mode: str = DEFAULT_INPUT_INJECT_MODE


@dataclass(frozen=True)
class ResolvedInput:
    kind: str = "unresolved"
    path: str = ""
    value: Any = None
    digest: str = ""
    source_output_kind: str = ""
    producer_stage_instance_id: str = ""
    schema_version: int = SchemaVersion.INPUT_CONTRACT


@dataclass(frozen=True)
class InputSource:
    kind: str
    stage: str = ""
    output: str = ""
    path: str = ""
    schema_version: int = SchemaVersion.INPUT_CONTRACT


@dataclass(frozen=True)
class InputBinding:
    input_name: str
    source: InputSource
    expects: str
    resolved: ResolvedInput = field(default_factory=ResolvedInput)
    inject: JsonObject = field(default_factory=dict)
    resolution: JsonObject = field(default_factory=dict)
    schema_version: int = SchemaVersion.INPUT_CONTRACT


def input_source_from_dict(payload: JsonObject | InputSource) -> InputSource:
    if isinstance(payload, InputSource):
        return payload
    values = required_record(payload, "input_source")
    require_schema(values, name="input_source", version=SchemaVersion.INPUT_CONTRACT)
    return InputSource(
        kind=required_string(values, "kind", label="input_source", non_empty=True),
        stage=required_string(values, "stage", label="input_source"),
        output=required_string(values, "output", label="input_source"),
        path=required_string(values, "path", label="input_source"),
    )


def resolved_input_from_dict(
    payload: JsonObject | ResolvedInput,
) -> ResolvedInput:
    if isinstance(payload, ResolvedInput):
        return payload
    values = required_record(payload, "resolved_input")
    require_schema(values, name="resolved_input", version=SchemaVersion.INPUT_CONTRACT)
    return ResolvedInput(
        kind=required_string(values, "kind", label="resolved_input", non_empty=True),
        path=required_string(values, "path", label="resolved_input"),
        value=required_json_value(values, "value", label="resolved_input"),
        digest=required_string(values, "digest", label="resolved_input"),
        source_output_kind=required_string(
            values, "source_output_kind", label="resolved_input"
        ),
        producer_stage_instance_id=required_string(
            values, "producer_stage_instance_id", label="resolved_input"
        ),
    )


def input_binding_from_dict(payload: JsonObject) -> InputBinding:
    values = required_record(payload, "input_binding")
    require_schema(values, name="input_binding", version=SchemaVersion.INPUT_CONTRACT)
    return InputBinding(
        input_name=required_string(
            values, "input_name", label="input_binding", non_empty=True
        ),
        source=input_source_from_dict(
            required_object(values, "source", label="input_binding")
        ),
        expects=required_string(
            values, "expects", label="input_binding", non_empty=True
        ),
        resolved=resolved_input_from_dict(
            required_object(values, "resolved", label="input_binding")
        ),
        inject=required_object(values, "inject", label="input_binding"),
        resolution=required_object(values, "resolution", label="input_binding"),
    )


def resolved_kind_for_output_kind(output_kind: str, cardinality: str = "one") -> str:
    if output_kind == OUTPUT_KIND_METRIC:
        return INPUT_EXPECTS_VALUE
    if (
        output_kind in {OUTPUT_KIND_FILES, OUTPUT_KIND_MANIFEST}
        or cardinality == "many"
    ):
        return INPUT_EXPECTS_MANIFEST
    return INPUT_EXPECTS_PATH


def resolved_kind_matches_expectation(kind: str, expects: str) -> bool:
    return kind == expects


def inject_mode_matches_expectation(mode: str, expects: str) -> bool:
    if expects in {INPUT_EXPECTS_PATH, INPUT_EXPECTS_MANIFEST}:
        return mode in {INPUT_INJECT_PATH, INPUT_INJECT_JSON}
    if expects == INPUT_EXPECTS_VALUE:
        return mode in {INPUT_INJECT_VALUE, INPUT_INJECT_JSON}
    return False


def resolved_payload_present(binding: InputBinding) -> bool:
    resolved = binding.resolved
    if resolved.kind in {INPUT_EXPECTS_PATH, INPUT_EXPECTS_MANIFEST}:
        return bool(resolved.path)
    if resolved.kind == INPUT_EXPECTS_VALUE:
        return resolved.value is not None
    return False


def input_injection_value(binding: InputBinding) -> str | None:
    resolved = binding.resolved
    mode = str(binding.inject.get("mode") or DEFAULT_INPUT_INJECT_MODE)
    if mode == INPUT_INJECT_PATH:
        return (
            resolved.path
            if resolved.kind in {INPUT_EXPECTS_PATH, INPUT_EXPECTS_MANIFEST}
            and resolved.path
            else None
        )
    if mode == INPUT_INJECT_VALUE:
        return None if resolved.kind != INPUT_EXPECTS_VALUE else str(resolved.value)
    if mode == INPUT_INJECT_JSON:
        payload = resolved.value if resolved.kind == INPUT_EXPECTS_VALUE else resolved
        return stable_json(payload)
    return None


def binding_is_ready_for_injection(binding: InputBinding) -> bool:
    if not resolved_payload_present(binding):
        return False
    if not resolved_kind_matches_expectation(binding.resolved.kind, binding.expects):
        return False
    mode = str(binding.inject.get("mode") or DEFAULT_INPUT_INJECT_MODE)
    if not inject_mode_matches_expectation(mode, binding.expects):
        return False
    return input_injection_value(binding) is not None
