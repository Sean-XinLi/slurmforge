from __future__ import annotations

from pathlib import Path

from ..io import read_json
from ..contracts import InputBinding, input_binding_from_dict, input_injection_value


def _input_bindings_path(run_dir: Path) -> Path:
    return run_dir / "input_bindings.json"


def bindings_from_file(run_dir: Path) -> tuple[InputBinding, ...]:
    payload = read_json(_input_bindings_path(run_dir))
    if "schema_version" not in payload:
        raise ValueError("input_bindings.schema_version is required")
    if int(payload["schema_version"]) != 1:
        raise ValueError(f"input_bindings.schema_version is not supported: {payload['schema_version']}")
    return tuple(input_binding_from_dict(dict(item)) for item in dict(payload.get("bindings") or {}).values())


def binding_injected_value(binding: InputBinding) -> str | None:
    return input_injection_value(binding)
