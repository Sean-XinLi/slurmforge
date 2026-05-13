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
    required_bool,
    required_json_value,
    required_nullable_bool,
    required_nullable_string,
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
class InputResolution:
    kind: str = "unresolved"
    state: str = "unresolved"
    reason: str = ""
    source_root: str = ""
    producer_root: str = ""
    producer_run_dir: str = ""
    producer_stage_instance_id: str = ""
    producer_run_id: str = ""
    producer_stage_name: str = ""
    output_name: str = ""
    output_path: str = ""
    output_digest: str = ""
    producer_digest: str = ""
    digest: str = ""
    selection_reason: str = ""
    searched_root: str = ""
    resolved_from_lineage_root: str = ""
    source_exists: bool | None = None
    source_role: str = ""
    path_kind: str = "file"
    lineage_ref: str = ""
    expected_digest: str = ""
    schema_version: int = SchemaVersion.INPUT_CONTRACT


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
    required: bool = False
    resolved: ResolvedInput = field(default_factory=ResolvedInput)
    inject: InputInjection = field(default_factory=InputInjection)
    resolution: InputResolution = field(default_factory=InputResolution)
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


def input_injection_from_dict(
    payload: JsonObject | InputInjection,
) -> InputInjection:
    if isinstance(payload, InputInjection):
        return payload
    values = required_record(payload, "input_injection")
    return InputInjection(
        flag=required_nullable_string(values, "flag", label="input_injection"),
        env=required_nullable_string(values, "env", label="input_injection"),
        mode=required_string(values, "mode", label="input_injection", non_empty=True),
    )


def input_resolution_from_dict(
    payload: JsonObject | InputResolution,
) -> InputResolution:
    if isinstance(payload, InputResolution):
        return payload
    values = required_record(payload, "input_resolution")
    version = require_schema(
        values, name="input_resolution", version=SchemaVersion.INPUT_CONTRACT
    )
    return InputResolution(
        kind=required_string(values, "kind", label="input_resolution", non_empty=True),
        state=required_string(values, "state", label="input_resolution"),
        reason=required_string(values, "reason", label="input_resolution"),
        source_root=required_string(values, "source_root", label="input_resolution"),
        producer_root=required_string(
            values, "producer_root", label="input_resolution"
        ),
        producer_run_dir=required_string(
            values, "producer_run_dir", label="input_resolution"
        ),
        producer_stage_instance_id=required_string(
            values, "producer_stage_instance_id", label="input_resolution"
        ),
        producer_run_id=required_string(
            values, "producer_run_id", label="input_resolution"
        ),
        producer_stage_name=required_string(
            values, "producer_stage_name", label="input_resolution"
        ),
        output_name=required_string(values, "output_name", label="input_resolution"),
        output_path=required_string(values, "output_path", label="input_resolution"),
        output_digest=required_string(
            values, "output_digest", label="input_resolution"
        ),
        producer_digest=required_string(
            values, "producer_digest", label="input_resolution"
        ),
        digest=required_string(values, "digest", label="input_resolution"),
        selection_reason=required_string(
            values, "selection_reason", label="input_resolution"
        ),
        searched_root=required_string(
            values, "searched_root", label="input_resolution"
        ),
        resolved_from_lineage_root=required_string(
            values, "resolved_from_lineage_root", label="input_resolution"
        ),
        source_exists=required_nullable_bool(
            values, "source_exists", label="input_resolution"
        ),
        source_role=required_string(values, "source_role", label="input_resolution"),
        path_kind=required_string(
            values, "path_kind", label="input_resolution", non_empty=True
        ),
        lineage_ref=required_string(values, "lineage_ref", label="input_resolution"),
        expected_digest=required_string(
            values, "expected_digest", label="input_resolution"
        ),
        schema_version=version,
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
        required=required_bool(values, "required", label="input_binding"),
        resolved=resolved_input_from_dict(
            required_object(values, "resolved", label="input_binding")
        ),
        inject=input_injection_from_dict(
            required_object(values, "inject", label="input_binding")
        ),
        resolution=input_resolution_from_dict(
            required_object(values, "resolution", label="input_binding")
        ),
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
    mode = binding.inject.mode or DEFAULT_INPUT_INJECT_MODE
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
    mode = binding.inject.mode or DEFAULT_INPUT_INJECT_MODE
    if not inject_mode_matches_expectation(mode, binding.expects):
        return False
    return input_injection_value(binding) is not None
