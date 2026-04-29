"""Input contract dataclasses shared by spec, planner, resolver, and executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..io import SchemaVersion, require_schema, stable_json

JsonObject = dict[str, Any]


@dataclass(frozen=True)
class InputInjection:
    flag: str | None = None
    env: str | None = None
    mode: str = "path"


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
    values = dict(payload)
    if "schema_version" in values:
        require_schema(
            values, name="input_source", version=SchemaVersion.INPUT_CONTRACT
        )
    return InputSource(
        kind=str(values["kind"]),
        stage=str(values.get("stage") or ""),
        output=str(values.get("output") or ""),
        path=str(values.get("path") or ""),
    )


def resolved_input_from_dict(
    payload: JsonObject | ResolvedInput | None,
) -> ResolvedInput:
    if isinstance(payload, ResolvedInput):
        return payload
    values = dict(payload or {})
    if values and "schema_version" in values:
        require_schema(
            values, name="resolved_input", version=SchemaVersion.INPUT_CONTRACT
        )
    return ResolvedInput(
        kind=str(values.get("kind") or "unresolved"),
        path=str(values.get("path") or ""),
        value=values.get("value"),
        digest=str(values.get("digest") or ""),
        source_output_kind=str(values.get("source_output_kind") or ""),
        producer_stage_instance_id=str(values.get("producer_stage_instance_id") or ""),
    )


def input_binding_from_dict(payload: JsonObject) -> InputBinding:
    require_schema(payload, name="input_binding", version=SchemaVersion.INPUT_CONTRACT)
    return InputBinding(
        input_name=str(payload["input_name"]),
        source=input_source_from_dict(dict(payload["source"])),
        expects=str(payload["expects"]),
        resolved=resolved_input_from_dict(payload.get("resolved")),
        inject=dict(payload.get("inject") or {}),
        resolution=dict(payload.get("resolution") or {}),
    )


def resolved_kind_for_output_kind(output_kind: str, cardinality: str = "one") -> str:
    if output_kind == "metric":
        return "value"
    if output_kind in {"files", "manifest"} or cardinality == "many":
        return "manifest"
    return "path"


def resolved_kind_matches_expectation(kind: str, expects: str) -> bool:
    return kind == expects


def inject_mode_matches_expectation(mode: str, expects: str) -> bool:
    if expects in {"path", "manifest"}:
        return mode in {"path", "json"}
    if expects == "value":
        return mode in {"value", "json"}
    return False


def resolved_payload_present(binding: InputBinding) -> bool:
    resolved = binding.resolved
    if resolved.kind in {"path", "manifest"}:
        return bool(resolved.path)
    if resolved.kind == "value":
        return resolved.value is not None
    return False


def input_injection_value(binding: InputBinding) -> str | None:
    resolved = binding.resolved
    mode = str(binding.inject.get("mode") or "path")
    if mode == "path":
        return (
            resolved.path
            if resolved.kind in {"path", "manifest"} and resolved.path
            else None
        )
    if mode == "value":
        return None if resolved.kind != "value" else str(resolved.value)
    if mode == "json":
        payload = resolved.value if resolved.kind == "value" else resolved
        return stable_json(payload)
    return None


def binding_is_ready_for_injection(binding: InputBinding) -> bool:
    if not resolved_payload_present(binding):
        return False
    if not resolved_kind_matches_expectation(binding.resolved.kind, binding.expects):
        return False
    mode = str(binding.inject.get("mode") or "path")
    if not inject_mode_matches_expectation(mode, binding.expects):
        return False
    return input_injection_value(binding) is not None


def resolved_input_from_output_ref(output: Any) -> ResolvedInput:
    if isinstance(output, dict):
        values = dict(output)
        output_name = str(values.get("output_name") or "")
        output_kind = str(values.get("kind") or "file")
        path = str(values.get("path") or "")
        digest = str(
            values.get("digest")
            or values.get("managed_digest")
            or values.get("source_digest")
            or ""
        )
        value = values.get("value")
        cardinality = str(values.get("cardinality") or "one")
        producer = str(values.get("producer_stage_instance_id") or "")
    else:
        output_name = str(getattr(output, "output_name", ""))
        output_kind = str(getattr(output, "kind", "file"))
        path = str(getattr(output, "path", ""))
        digest = str(
            getattr(output, "digest", "")
            or getattr(output, "managed_digest", "")
            or getattr(output, "source_digest", "")
        )
        value = getattr(output, "value", None)
        cardinality = str(getattr(output, "cardinality", "one"))
        producer = str(getattr(output, "producer_stage_instance_id", ""))
    resolved_kind = resolved_kind_for_output_kind(output_kind, cardinality)
    if resolved_kind == "value":
        return ResolvedInput(
            kind="value",
            path=path,
            value=value,
            digest=digest,
            source_output_kind=output_kind,
            producer_stage_instance_id=producer,
        )
    return ResolvedInput(
        kind=resolved_kind,
        path=path,
        digest=digest,
        source_output_kind=output_kind or output_name,
        producer_stage_instance_id=producer,
    )
