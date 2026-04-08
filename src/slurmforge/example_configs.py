from __future__ import annotations

from pathlib import Path

from .errors import ConfigContractError
from .resource_io import list_package_files, read_package_text
from .starter_catalog import list_starter_specs

_EXAMPLES_PACKAGE = "slurmforge.examples"

# Additional examples that exist in the examples/ directory but are not generated
# by sforge init (referenced by sforge examples list/show/export).
_EXTRA_DESCRIPTIONS: dict[str, str] = {
    "adapter_minimal":  "Minimal runnable adapter-mode config for advanced interface bridging.",
    "command_minimal":  "Minimal runnable command-mode config for wrapping an existing training command.",
    "model_registry":   "Registry file referenced by the registry starter (models.yaml format).",
}

# Short one-line descriptions used by `sforge examples list`.
_LIST_DESCRIPTIONS: dict[str, str] = {
    "command_starter":  "Single-GPU run from your existing command",
    "command_hpc":      "Multi-GPU / cluster run (Slurm)",
    "command_minimal":  "Minimal command example",
    "script_starter":   "Single-GPU run with your own script",
    "script_hpc":       "Multi-GPU / distributed run (Slurm)",
    "adapter_starter":  "Single-GPU adapter pipeline",
    "adapter_hpc":      "Multi-GPU adapter on cluster",
    "adapter_minimal":  "Minimal adapter example",
    "registry_starter": "Single-GPU multi-model workflow",
    "registry_hpc":     "Multi-GPU multi-model runs on cluster",
    "model_registry":   "Registry definition file",
}

_QUICK_START: tuple[str, str] = (
    "command_starter",
    "Run your training on a single GPU (local or simple setup)",
)

_GROUPS: list[tuple[str, list[str]]] = [
    ("Command mode (use existing code)", ["command_starter", "command_hpc", "command_minimal"]),
    ("Script mode (write your own)",     ["script_starter", "script_hpc"]),
    ("Adapter mode (advanced)",          ["adapter_starter", "adapter_hpc", "adapter_minimal"]),
    ("Registry mode (multi-model)",      ["registry_starter", "registry_hpc", "model_registry"]),
]


def _build_descriptions() -> dict[str, str]:
    descriptions: dict[str, str] = {
        s.example_name: s.post_init_guidance for s in list_starter_specs()
    }
    descriptions.update(_EXTRA_DESCRIPTIONS)
    return descriptions


def list_example_files() -> list[str]:
    return list_package_files(_EXAMPLES_PACKAGE, suffix=".yaml")


def list_example_names() -> list[str]:
    return [Path(filename).stem for filename in list_example_files()]


def list_example_catalog() -> list[tuple[str, str]]:
    descriptions = _build_descriptions()
    return [(name, descriptions.get(name, "")) for name in list_example_names()]


def _resolve_example_filename(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ConfigContractError("example name must not be empty")

    available = {filename: filename for filename in list_example_files()}
    if normalized in available:
        return normalized

    candidate = f"{normalized}.yaml"
    if candidate in available:
        return candidate

    raise ConfigContractError(
        f"unknown example {normalized!r}; available examples: {', '.join(list_example_names())}"
    )


def read_example_text(name: str) -> str:
    filename = _resolve_example_filename(name)
    return read_package_text(_EXAMPLES_PACKAGE, filename)


def get_example_list_view() -> dict[str, object]:
    """Return structured data for the grouped `sforge examples list` display."""
    available = set(list_example_names())
    groups = [
        (heading, [(n, _LIST_DESCRIPTIONS.get(n, "")) for n in names if n in available])
        for heading, names in _GROUPS
    ]
    return {"quick_start": _QUICK_START, "groups": groups}


def export_example(name: str, output_path: Path, *, force: bool = False) -> Path:
    path = output_path.expanduser().resolve()
    if path.exists() and not force:
        raise FileExistsError(f"refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(read_example_text(name), encoding="utf-8")
    return path
