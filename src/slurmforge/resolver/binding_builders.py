from __future__ import annotations

from pathlib import Path

from ..contracts import (
    InputBinding,
    InputInjection,
    InputResolution,
    InputSource,
    ResolvedInput,
)
from ..errors import ConfigContractError
from ..spec import ExperimentSpec, StageInputSpec


def input_inject(
    spec: ExperimentSpec, *, stage_name: str, input_name: str
) -> InputInjection:
    input_spec = spec.enabled_stages[stage_name].inputs.get(input_name)
    if input_spec is None:
        raise ConfigContractError(
            f"`stages.{stage_name}.inputs.{input_name}` is required"
        )
    return inject_payload(input_spec)


def source_payload(input_spec: StageInputSpec) -> InputSource:
    source = input_spec.source
    return InputSource(
        kind=source.kind, stage=source.stage, output=source.output, path=source.path
    )


def inject_payload(input_spec: StageInputSpec) -> InputInjection:
    return InputInjection(
        flag=input_spec.inject.flag,
        env=input_spec.inject.env,
        mode=input_spec.inject.mode,
    )


def unresolved_resolved(*, path: str = "") -> ResolvedInput:
    return ResolvedInput(kind="unresolved", path=path)


def unresolved_binding(
    input_spec: StageInputSpec, *, source: InputSource | None = None, reason: str
) -> InputBinding:
    actual_source = source or source_payload(input_spec)
    return InputBinding(
        input_name=input_spec.name,
        source=actual_source,
        expects=input_spec.expects,
        required=input_spec.required,
        resolved=unresolved_resolved(),
        inject=inject_payload(input_spec),
        resolution=InputResolution(
            kind=actual_source.kind,
            state="unresolved",
            reason=reason,
        ),
    )


def path_binding_for_spec(
    spec: ExperimentSpec, input_spec: StageInputSpec
) -> InputBinding:
    source_path = Path(input_spec.source.path).expanduser()
    resolved = (
        source_path if source_path.is_absolute() else spec.project_root / source_path
    )
    path_text = str(resolved.resolve())
    return InputBinding(
        input_name=input_spec.name,
        source=source_payload(input_spec),
        expects=input_spec.expects,
        required=input_spec.required,
        resolved=ResolvedInput(kind=input_spec.expects, path=path_text),
        inject=inject_payload(input_spec),
        resolution=InputResolution(
            kind=input_spec.source.kind,
            state="resolved",
            source_root=str(spec.project_root),
        ),
    )


def path_binding_for_input(
    *,
    input_name: str,
    inject: InputInjection,
    source: InputSource,
    expects: str,
    required: bool,
    resolved: ResolvedInput,
    resolution: InputResolution,
) -> InputBinding:
    return InputBinding(
        input_name=input_name,
        source=source,
        expects=expects,
        required=required,
        resolved=resolved,
        inject=inject,
        resolution=resolution,
    )
