from __future__ import annotations

from pathlib import Path

from ..contracts import InputBinding, InputResolution, ResolvedInput
from ..contracts import RunDefinition
from ..spec import ExperimentSpec, StageSpec
from .binding_builders import inject_payload, source_payload, unresolved_resolved


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
        resolution = InputResolution(kind=source.kind, state="unresolved")
        if source.kind == "upstream_output":
            upstream_stage, output_name = source.stage, source.output
            resolution = InputResolution(
                kind="upstream_output",
                state="awaiting_upstream_output",
                producer_stage_instance_id=f"{upstream_stage}/{run.run_id}",
                output_name=output_name,
            )
        elif source.kind == "external_path":
            source_path = Path(source.path).expanduser()
            resolved = (
                source_path
                if source_path.is_absolute()
                else spec.project_root / source_path
            )
            path_text = str(resolved.resolve())
            resolved_payload = ResolvedInput(kind=input_spec.expects, path=path_text)
            resolution = InputResolution(
                kind=source.kind,
                state="resolved",
                source_root=str(spec.project_root),
            )
        bindings.append(
            InputBinding(
                input_name=input_spec.name,
                source=source,
                expects=input_spec.expects,
                required=input_spec.required,
                resolved=resolved_payload,
                inject=inject_payload(input_spec),
                resolution=resolution,
            )
        )
    return tuple(bindings)
