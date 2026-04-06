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


def export_example(name: str, output_path: Path, *, force: bool = False) -> Path:
    path = output_path.expanduser().resolve()
    if path.exists() and not force:
        raise FileExistsError(f"refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(read_example_text(name), encoding="utf-8")
    return path
