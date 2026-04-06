from __future__ import annotations

from pathlib import Path

from .errors import ConfigContractError
from .example_configs import read_example_text
from .resource_io import read_package_text
from .starter_catalog import StarterResource, get_starter_spec

_STARTER_TEMPLATES_PACKAGE = "slurmforge.starter_templates"


def _read_starter_template(
    template_name: str,
    *,
    replacements: tuple[tuple[str, str], ...] = (),
) -> str:
    text = read_package_text(_STARTER_TEMPLATES_PACKAGE, template_name)
    for key, value in replacements:
        text = text.replace(key, value)
    return text


def _materialize_resource(resource: StarterResource) -> str | None:
    if resource.kind == "directory":
        return None
    if resource.kind == "example":
        if not resource.source_name:
            raise ConfigContractError(f"starter resource {resource.relative_path!r} is missing example source_name")
        return read_example_text(resource.source_name)
    if resource.kind == "template":
        if not resource.source_name:
            raise ConfigContractError(f"starter resource {resource.relative_path!r} is missing template source_name")
        return _read_starter_template(resource.source_name, replacements=resource.replacements)
    raise ConfigContractError(f"unsupported starter resource kind: {resource.kind}")


def init_project(
    template_type: str,
    profile: str,
    output_dir: Path,
    *,
    force: bool = False,
) -> list[Path]:
    spec = get_starter_spec(template_type, profile)
    root = output_dir.expanduser().resolve()
    if root.exists():
        if not root.is_dir():
            raise NotADirectoryError(f"starter project root is not a directory: {root}")
        if any(root.iterdir()) and not force:
            raise FileExistsError(f"refusing to initialize into non-empty directory: {root}")
    else:
        root.mkdir(parents=True, exist_ok=True)

    targets = [root / resource.relative_path for resource in spec.resources]
    for target in targets:
        if target.exists() and not force:
            raise FileExistsError(f"refusing to overwrite existing path: {target}")

    written: list[Path] = []
    for resource, target in zip(spec.resources, targets):
        content = _materialize_resource(resource)
        if content is None:
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        written.append(target.resolve())
    return written
