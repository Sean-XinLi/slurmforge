from __future__ import annotations

from pathlib import Path

from ..contracts import InputBinding, input_binding_from_dict, input_injection_value
from ..io import SchemaVersion, read_json, require_schema
from ..record_fields import required_object, required_record


def _input_bindings_path(run_dir: Path) -> Path:
    return run_dir / "input_bindings.json"


def bindings_from_file(run_dir: Path) -> tuple[InputBinding, ...]:
    payload = read_json(_input_bindings_path(run_dir))
    require_schema(payload, name="input_bindings", version=SchemaVersion.INPUT_BINDINGS)
    bindings = required_object(payload, "bindings", label="input_bindings")
    return tuple(
        input_binding_from_dict(
            required_record(item, f"input_bindings.bindings.{name}")
        )
        for name, item in bindings.items()
    )


def binding_injected_value(binding: InputBinding) -> str | None:
    return input_injection_value(binding)
