from __future__ import annotations

from typing import Final

FIELD_OPTIONS: Final[dict[str, tuple[str, ...]]] = {
    "artifact_store.fallback_strategy": (
        "null",
        "copy",
        "hardlink",
        "symlink",
        "register_only",
    ),
    "artifact_store.strategy": ("copy", "hardlink", "symlink", "register_only"),
    "dispatch.overflow_policy": ("serialize_groups", "error", "best_effort"),
    "runs.type": ("single", "grid", "cases", "matrix"),
    "stages.*.entry.type": ("python_script", "command"),
    "stages.*.inputs.*.expects": ("path", "manifest", "value"),
    "stages.*.inputs.*.inject.mode": ("path", "value", "json"),
    "stages.*.inputs.*.source.kind": ("upstream_output", "external_path"),
    "stages.*.launcher.mode": ("single_node", "multi_node"),
    "stages.*.launcher.type": (
        "single",
        "python",
        "torchrun",
        "srun",
        "mpirun",
        "command",
    ),
    "stages.*.outputs.*.discover.select": ("latest_step", "first", "last"),
    "stages.*.outputs.*.kind": ("file", "files", "metric", "manifest"),
}


def options_for(field: str) -> tuple[str, ...]:
    return FIELD_OPTIONS[field]


def options_csv(field: str) -> str:
    return ", ".join(options_for(field))


def options_sentence(field: str) -> str:
    return _sentence_join(options_for(field))


def options_comment(field: str, *, indent: int) -> str:
    return f"{' ' * indent}# Options: {options_csv(field)}."


def _sentence_join(values: tuple[str, ...]) -> str:
    if len(values) <= 1:
        return "".join(values)
    return f"{', '.join(values[:-1])}, or {values[-1]}"
