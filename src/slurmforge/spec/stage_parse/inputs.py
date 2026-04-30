from __future__ import annotations

from typing import Any

from ...config_contract.option_sets import (
    INPUT_SOURCE_EXTERNAL_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
)
from ...config_contract.registry import default_for, options_for, options_sentence
from ...config_schema import reject_unknown_config_keys
from ...contracts import InputInjection, InputSource
from ...errors import ConfigContractError
from ..models import StageInputSpec
from ..parse_common import optional_mapping, require_mapping

DEFAULT_INPUT_EXPECTS = default_for("stages.*.inputs.*.expects")
DEFAULT_INPUT_INJECT_MODE = default_for("stages.*.inputs.*.inject.mode")


def parse_inputs(raw: Any, *, stage_name: str) -> dict[str, StageInputSpec]:
    data = optional_mapping(raw, f"stages.{stage_name}.inputs")
    parsed: dict[str, StageInputSpec] = {}
    for input_name, input_raw in data.items():
        input_data = optional_mapping(
            input_raw, f"stages.{stage_name}.inputs.{input_name}"
        )
        reject_unknown_config_keys(
            input_data,
            parent=f"stages.{stage_name}.inputs.{input_name}",
        )
        inject_data = optional_mapping(
            input_data.get("inject"), f"stages.{stage_name}.inputs.{input_name}.inject"
        )
        reject_unknown_config_keys(
            inject_data,
            parent=f"stages.{stage_name}.inputs.{input_name}.inject",
        )
        source_data = require_mapping(
            input_data.get("source"), f"stages.{stage_name}.inputs.{input_name}.source"
        )
        source = _parse_input_source(
            source_data, stage_name=stage_name, input_name=str(input_name)
        )
        expects = str(input_data.get("expects") or DEFAULT_INPUT_EXPECTS)
        if expects not in options_for("stages.*.inputs.*.expects"):
            raise ConfigContractError(
                f"`stages.{stage_name}.inputs.{input_name}.expects` must be "
                f"{options_sentence('stages.*.inputs.*.expects')}"
            )
        parsed[str(input_name)] = StageInputSpec(
            name=str(input_name),
            source=source,
            expects=expects,
            required=bool(input_data.get("required", False)),
            inject=InputInjection(
                flag=None
                if inject_data.get("flag") in (None, "")
                else str(inject_data.get("flag")),
                env=None
                if inject_data.get("env") in (None, "")
                else str(inject_data.get("env")),
                mode=str(inject_data.get("mode") or DEFAULT_INPUT_INJECT_MODE),
            ),
        )
    return parsed


def _parse_input_source(
    source_data: dict[str, Any], *, stage_name: str, input_name: str
) -> InputSource:
    reject_unknown_config_keys(
        source_data,
        parent=f"stages.{stage_name}.inputs.{input_name}.source",
    )
    kind = str(source_data.get("kind") or "")
    if kind not in options_for("stages.*.inputs.*.source.kind"):
        raise ConfigContractError(
            f"`stages.{stage_name}.inputs.{input_name}.source.kind` must be "
            f"{options_sentence('stages.*.inputs.*.source.kind')}"
        )
    if kind == INPUT_SOURCE_UPSTREAM_OUTPUT:
        if source_data.get("stage") in (None, ""):
            raise ConfigContractError(
                f"`stages.{stage_name}.inputs.{input_name}.source.stage` is required"
            )
        if source_data.get("output") in (None, ""):
            raise ConfigContractError(
                f"`stages.{stage_name}.inputs.{input_name}.source.output` is required"
            )
        return InputSource(
            kind=INPUT_SOURCE_UPSTREAM_OUTPUT,
            stage=str(source_data["stage"]),
            output=str(source_data["output"]),
        )
    if source_data.get("path") in (None, ""):
        raise ConfigContractError(
            f"`stages.{stage_name}.inputs.{input_name}.source.path` is required"
        )
    return InputSource(kind=INPUT_SOURCE_EXTERNAL_PATH, path=str(source_data["path"]))
