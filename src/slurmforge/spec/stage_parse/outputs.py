from __future__ import annotations

from typing import Any

from ...config_contract.defaults import (
    DEFAULT_OUTPUT_JSON_PATH,
    DEFAULT_OUTPUT_REQUIRED,
)
from ...config_contract.options import OUTPUT_KIND_FILE, options_for, options_sentence
from ...config_schema import reject_unknown_config_keys
from ...contracts.output_selectors import normalize_output_selector
from ...contracts.outputs import (
    FileOutputDiscoveryRule,
    OutputDiscoveryRule,
    StageOutputContract,
    StageOutputSpec,
)
from ...errors import ConfigContractError

OUTPUT_KINDS = set(options_for("stages.*.outputs.*.kind"))


def parse_stage_output_config(raw: Any, *, stage_name: str) -> StageOutputContract:
    if raw in (None, ""):
        return StageOutputContract()
    data = _require_mapping(raw, f"stages.{stage_name}.outputs")
    outputs = {
        str(output_name): _parse_output_spec(
            str(output_name), output_raw, stage_name=stage_name
        )
        for output_name, output_raw in sorted(data.items())
    }
    return StageOutputContract(outputs=outputs)


def _parse_output_spec(
    output_name: str, raw: Any, *, stage_name: str
) -> StageOutputSpec:
    name = f"stages.{stage_name}.outputs.{output_name}"
    data = _require_mapping(raw, name)
    reject_unknown_config_keys(data, parent=name)
    kind = str(data.get("kind") or "")
    if kind not in OUTPUT_KINDS:
        raise ConfigContractError(
            f"`{name}.kind` must be {options_sentence('stages.*.outputs.*.kind')}"
        )
    discover = _parse_discovery(
        data.get("discover"),
        name=f"{name}.discover",
        allow_select=kind == OUTPUT_KIND_FILE,
    )
    file_value = data.get("file")
    return StageOutputSpec(
        name=output_name,
        kind=kind,
        required=bool(data.get("required", DEFAULT_OUTPUT_REQUIRED)),
        discover=discover,
        file="" if file_value in (None, "") else str(file_value),
        json_path=str(data.get("json_path") or DEFAULT_OUTPUT_JSON_PATH),
    )


def _parse_discovery(
    raw: Any, *, name: str, allow_select: bool
) -> OutputDiscoveryRule | FileOutputDiscoveryRule:
    data = {} if raw in (None, "") else _require_mapping(raw, name)
    reject_unknown_config_keys(data, parent=name)
    globs = _string_tuple(data.get("globs"), name=f"{name}.globs")
    if "select" in data:
        if not allow_select:
            raise ConfigContractError(
                f"`{name}.select` is only supported for file outputs"
            )
        return FileOutputDiscoveryRule(
            globs=globs, select=normalize_output_selector(data.get("select"))
        )
    if allow_select:
        return FileOutputDiscoveryRule(globs=globs)
    return OutputDiscoveryRule(globs=globs)


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigContractError(f"`{name}` must be a mapping")
    return value


def _string_tuple(value: Any, *, name: str) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        items = (value,)
    elif isinstance(value, (list, tuple)):
        items = tuple(value)
    else:
        raise ConfigContractError(f"`{name}` must be a string or list of strings")
    if not all(isinstance(item, str) and item for item in items):
        raise ConfigContractError(f"`{name}` must contain non-empty strings")
    return tuple(str(item) for item in items)
