from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..errors import ConfigContractError
from ..io import SchemaVersion
from ..plans import RunDefinition
from ..schema import (
    InputBinding,
    InputSource,
    ResolvedInput,
    resolved_input_from_output_ref,
)
from ..spec import ExperimentSpec, StageInputSpec, StageSpec


@dataclass(frozen=True)
class ResolvedStageInputs:
    stage_name: str
    selected_runs: tuple[RunDefinition, ...]
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]]
    blocked_run_ids: tuple[str, ...] = ()
    blocked_reasons: dict[str, str] = field(default_factory=dict)
    schema_version: int = SchemaVersion.INPUT_CONTRACT


def input_inject(spec: ExperimentSpec, *, stage_name: str, input_name: str) -> dict[str, object]:
    input_spec = spec.enabled_stages[stage_name].inputs.get(input_name)
    if input_spec is None:
        raise ConfigContractError(f"`stages.{stage_name}.inputs.{input_name}` is required")
    return {
        "flag": input_spec.inject.flag,
        "env": input_spec.inject.env,
        "mode": input_spec.inject.mode,
        "required": input_spec.required,
    }


def source_payload(input_spec: StageInputSpec) -> InputSource:
    source = input_spec.source
    return InputSource(kind=source.kind, stage=source.stage, output=source.output, path=source.path)


def inject_payload(input_spec: StageInputSpec) -> dict[str, object]:
    return {
        "flag": input_spec.inject.flag,
        "env": input_spec.inject.env,
        "mode": input_spec.inject.mode,
        "required": input_spec.required,
    }


def path_resolved(path: str, *, digest: str = "", source_output_kind: str = "") -> ResolvedInput:
    return ResolvedInput(kind="path", path=path, digest=digest, source_output_kind=source_output_kind)


def unresolved_resolved(*, path: str = "") -> ResolvedInput:
    return ResolvedInput(kind="unresolved", path=path)


def resolved_output(output: dict) -> ResolvedInput:
    return resolved_input_from_output_ref(output)


def unresolved_binding(input_spec: StageInputSpec, *, source: InputSource | None = None, reason: str) -> InputBinding:
    actual_source = source or source_payload(input_spec)
    return InputBinding(
        input_name=input_spec.name,
        source=actual_source,
        expects=input_spec.expects,
        resolved=unresolved_resolved(),
        inject=inject_payload(input_spec),
        resolution={"kind": actual_source.kind, "state": "unresolved", "reason": reason},
    )


def path_binding_for_spec(spec: ExperimentSpec, input_spec: StageInputSpec) -> InputBinding:
    source_path = Path(input_spec.source.path).expanduser()
    resolved = source_path if source_path.is_absolute() else spec.project_root / source_path
    path_text = str(resolved.resolve())
    return InputBinding(
        input_name=input_spec.name,
        source=source_payload(input_spec),
        expects=input_spec.expects,
        resolved=ResolvedInput(kind=input_spec.expects, path=path_text),
        inject=inject_payload(input_spec),
        resolution={
            "kind": input_spec.source.kind,
            "resolved": {"kind": "path", "path": path_text},
            "source_root": str(spec.project_root),
        },
    )


def output_ref(payload: dict, output_name: str) -> dict | None:
    outputs = dict(payload.get("outputs") or {})
    output = outputs.get(output_name)
    if isinstance(output, dict) and output.get("path"):
        return dict(output)
    return None


def producer_root_from_run_dir(run_dir: Path) -> Path:
    if run_dir.parent.name == "runs":
        return run_dir.parent.parent.resolve()
    return run_dir.parent.resolve()


def upstream_resolution(
    *,
    producer_root: Path,
    run_dir: Path,
    stage_instance_id: str,
    run_id: str,
    stage_name: str,
    output_name: str,
    output: dict,
) -> dict[str, object]:
    return {
        "kind": "upstream_output",
        "producer_root": str(producer_root.resolve()),
        "producer_run_dir": str(run_dir.resolve()),
        "producer_stage_instance_id": stage_instance_id,
        "producer_run_id": run_id,
        "producer_stage_name": stage_name,
        "output_name": output_name,
        "output_path": str(output["path"]),
        "output_digest": str(output.get("digest") or output.get("managed_digest") or ""),
        "selection_reason": str(output.get("selection_reason") or ""),
    }


def path_binding_for_input(
    *,
    input_name: str,
    inject: dict[str, object],
    source: InputSource,
    expects: str,
    resolved: ResolvedInput,
    resolution: dict[str, object],
) -> InputBinding:
    return InputBinding(
        input_name=input_name,
        source=source,
        expects=expects,
        resolved=resolved,
        inject=inject,
        resolution=resolution,
    )


def producer_output_for_input(input_spec: StageInputSpec, *, producer_stage_name: str) -> tuple[str, str]:
    if input_spec.source.kind == "upstream_output":
        upstream_stage = input_spec.source.stage
        output_name = input_spec.source.output
        if upstream_stage != producer_stage_name:
            raise ConfigContractError(
                f"`{input_spec.name}` expects upstream stage `{upstream_stage}`, "
                f"but source batch is `{producer_stage_name}`"
            )
        return upstream_stage, output_name
    raise ConfigContractError(
        f"`{input_spec.name}` source kind `{input_spec.source.kind}` cannot be bound from an upstream batch"
    )


def default_stage_input_bindings(
    spec: ExperimentSpec,
    run: RunDefinition,
    stage: StageSpec,
) -> tuple[InputBinding, ...]:
    bindings: list[InputBinding] = []
    for name in sorted(stage.inputs):
        input_spec = stage.inputs[name]
        source = source_payload(input_spec)
        resolved_payload = unresolved_resolved()
        resolution: dict[str, object] = {"kind": source.kind, "state": "unresolved"}
        if source.kind == "upstream_output":
            upstream_stage, output_name = source.stage, source.output
            resolution = {
                "kind": "upstream_output",
                "state": "awaiting_upstream_output",
                "producer_stage_instance_id": f"{run.run_id}.{upstream_stage}",
                "output_name": output_name,
            }
        elif source.kind == "external_path":
            source_path = Path(source.path).expanduser()
            resolved = source_path if source_path.is_absolute() else spec.project_root / source_path
            path_text = str(resolved.resolve())
            resolved_payload = ResolvedInput(kind=input_spec.expects, path=path_text)
            resolution = {
                "kind": source.kind,
                "resolved": {"kind": input_spec.expects, "path": path_text},
                "source_root": str(spec.project_root),
            }
        bindings.append(
            InputBinding(
                input_name=input_spec.name,
                source=source,
                expects=input_spec.expects,
                resolved=resolved_payload,
                inject=inject_payload(input_spec),
                resolution=resolution,
            )
        )
    return tuple(bindings)
