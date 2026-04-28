from __future__ import annotations

from pathlib import Path

from ...contracts import InputBinding, InputSource, ResolvedInput
from ...errors import ConfigContractError
from ...contracts import RunDefinition
from ...spec import ExperimentSpec
from ...spec.queries import stage_name_for_kind
from ...spec.run_expansion import expand_run_definitions
from ..binding_builders import input_inject


def explicit_input_bindings(
    spec: ExperimentSpec,
    input_name: str,
    path: Path,
    *,
    stage_name: str | None = None,
    runs: tuple[RunDefinition, ...] | None = None,
    source_role: str = "",
) -> tuple[tuple[RunDefinition, ...], dict[str, tuple[InputBinding, ...]]]:
    consumer_stage = stage_name or stage_name_for_kind(spec, "eval")
    input_spec = spec.enabled_stages[consumer_stage].inputs.get(input_name)
    if input_spec is None:
        raise ConfigContractError(f"`stages.{consumer_stage}.inputs.{input_name}` is required")
    if input_spec.expects not in {"path", "manifest"}:
        raise ConfigContractError(f"`{input_name}` expects {input_spec.expects}; explicit path inputs require path or manifest")
    source_path = path.expanduser()
    resolved = source_path.resolve() if source_path.is_absolute() else (spec.project_root / source_path).resolve()
    if not resolved.exists() or not resolved.is_file():
        raise ConfigContractError(f"Input path does not exist: {resolved}")
    selected_runs = runs or expand_run_definitions(spec)
    inject = input_inject(spec, stage_name=consumer_stage, input_name=input_name)
    resolution = {
        "kind": "external_path",
        "resolved": {"kind": input_spec.expects, "path": str(resolved)},
        "source_exists": True,
    }
    if source_role:
        resolution["source_role"] = source_role
    bindings = {
        run.run_id: (
            InputBinding(
                input_name=input_name,
                source=InputSource(kind="external_path", path=str(resolved)),
                expects=input_spec.expects,
                resolved=ResolvedInput(kind=input_spec.expects, path=str(resolved)),
                inject=inject,
                resolution=resolution,
            ),
        )
        for run in selected_runs
    }
    return selected_runs, bindings
