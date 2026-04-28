from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..contracts import inject_mode_matches_expectation, resolved_kind_for_output_kind
from .models import ExperimentSpec, StageSpec


def validate_stage_inputs_contract(spec: ExperimentSpec, stage: StageSpec, *, check_paths: bool) -> None:
    if stage.depends_on and not any(input_spec.required for input_spec in stage.inputs.values()):
        raise ConfigContractError(f"`stages.{stage.name}` depends on upstream stages but declares no required inputs")
    for input_name, input_spec in stage.inputs.items():
        if input_spec.inject.mode not in {"path", "value", "json"}:
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.inject.mode` must be path, value, or json"
            )
        if not inject_mode_matches_expectation(input_spec.inject.mode, input_spec.expects):
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.inject.mode` is not compatible with "
                f"expects={input_spec.expects}"
            )
        if input_spec.source.kind == "upstream_output":
            _validate_upstream_input(spec, stage, input_name)
        elif input_spec.source.kind == "external_path":
            _validate_external_input(spec, stage, input_name, check_paths=check_paths)
        else:
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.source.kind` is not supported: {input_spec.source.kind}"
            )


def _validate_upstream_input(spec: ExperimentSpec, stage: StageSpec, input_name: str) -> None:
    input_spec = stage.inputs[input_name]
    upstream_stage = input_spec.source.stage
    output_name = input_spec.source.output
    if not upstream_stage:
        raise ConfigContractError(f"`stages.{stage.name}.inputs.{input_name}.source.stage` is required")
    if upstream_stage not in spec.enabled_stages:
        raise ConfigContractError(
            f"`stages.{stage.name}.inputs.{input_name}.source.stage` references unknown stage `{upstream_stage}`"
        )
    if not output_name:
        raise ConfigContractError(f"`stages.{stage.name}.inputs.{input_name}.source.output` is required")
    upstream_outputs = spec.enabled_stages[upstream_stage].outputs.outputs
    if output_name not in upstream_outputs:
        raise ConfigContractError(
            f"`stages.{stage.name}.inputs.{input_name}.source.output` references missing output "
            f"`{upstream_stage}.{output_name}`"
        )
    output_kind = upstream_outputs[output_name].kind
    expected_kind = resolved_kind_for_output_kind(output_kind)
    if input_spec.expects != expected_kind:
        raise ConfigContractError(
            f"`stages.{stage.name}.inputs.{input_name}.expects={input_spec.expects}` is not compatible "
            f"with output `{upstream_stage}.{output_name}` kind={output_kind}; expected {expected_kind}"
        )
    if stage.depends_on and upstream_stage not in stage.depends_on:
        raise ConfigContractError(
            f"`stages.{stage.name}.inputs.{input_name}.source.stage` must reference one of "
            f"the stage dependencies: {', '.join(stage.depends_on)}"
        )


def _validate_external_input(
    spec: ExperimentSpec,
    stage: StageSpec,
    input_name: str,
    *,
    check_paths: bool,
) -> None:
    input_spec = stage.inputs[input_name]
    if input_spec.expects == "value":
        raise ConfigContractError(f"`stages.{stage.name}.inputs.{input_name}.expects=value` cannot use external_path")
    if check_paths:
        source_path = Path(input_spec.source.path).expanduser()
        source_path = source_path if source_path.is_absolute() else spec.project_root / source_path
        if not source_path.exists():
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.source.path` does not exist: {source_path}"
            )
