from __future__ import annotations

from pathlib import Path

from ..contracts import (
    InputBinding,
    ResolvedInput,
    binding_is_ready_for_injection,
    input_source_from_dict,
    resolved_input_from_dict,
)
from ..errors import ConfigContractError
from ..contracts import RunDefinition
from ..spec import ExperimentSpec
from .binding_builders import inject_payload, source_payload, unresolved_resolved
from .upstream import find_upstream_output


def resolve_stage_inputs_from_prior_source(
    *,
    spec: ExperimentSpec,
    source_root: Path,
    stage_name: str,
    run: RunDefinition,
) -> tuple[InputBinding, ...]:
    stage = spec.enabled_stages[stage_name]
    bindings: list[InputBinding] = []
    for input_name in sorted(stage.inputs):
        input_spec = stage.inputs[input_name]
        source = source_payload(input_spec)
        resolved_payload = unresolved_resolved()
        resolution = {"kind": source.kind, "state": "unresolved"}
        if source.kind == "upstream_output":
            upstream_stage, output_name = source.stage, source.output
            lineage_ref = f"{upstream_stage}/{run.run_id}:{output_name}"
            resolved = find_upstream_output(
                source_root,
                lineage_ref,
                run_id=run.run_id,
                input_name=input_name,
            )
            if resolved is not None:
                source = input_source_from_dict(resolved["source"])
                resolved_payload = resolved_input_from_dict(resolved.get("resolved"))
                resolution = dict(resolved["resolution"])
        elif source.kind == "external_path":
            source_path = Path(source.path).expanduser()
            source_path = (
                source_path
                if source_path.is_absolute()
                else spec.project_root / source_path
            )
            source_path = source_path.resolve()
            resolved_payload = (
                ResolvedInput(kind=input_spec.expects, path=str(source_path))
                if source_path.exists()
                else unresolved_resolved(path=str(source_path))
            )
            resolution = {
                "kind": source.kind,
                "resolved": {
                    "kind": resolved_payload.kind,
                    "path": resolved_payload.path,
                },
                "source_exists": source_path.exists(),
            }
        binding = InputBinding(
            input_name=input_spec.name,
            source=source,
            expects=input_spec.expects,
            resolved=resolved_payload,
            inject=inject_payload(input_spec),
            resolution=resolution,
        )
        if input_spec.required and not binding_is_ready_for_injection(binding):
            raise ConfigContractError(
                f"Cannot bind `{input_name}` for run `{run.run_id}` from prior source: "
                "source did not resolve for injection"
            )
        bindings.append(binding)
    return tuple(bindings)
