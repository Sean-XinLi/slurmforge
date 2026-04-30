from __future__ import annotations

from pathlib import Path

from .config_yaml import render_starter_config
from ..config_contract.default_values import DEFAULT_CONFIG_FILENAME
from .errors import StarterTemplateError
from .models import InitRequest, RenderedFile, StarterTemplate
from .templates.config_guide import render_starter_config_guide
from .templates.readme import render_starter_readme


def render_starter_files(
    request: InitRequest, template: StarterTemplate
) -> tuple[RenderedFile, ...]:
    root = request.output_dir.resolve()
    config_path = root / DEFAULT_CONFIG_FILENAME
    rendered = [
        RenderedFile(
            path=config_path,
            content=render_starter_config(
                template.name,
                template.config_builder(request),
            ),
            role="config",
        ),
        RenderedFile(
            path=root / "README.sforge.md",
            content=render_starter_readme(template.readme_builder(request)),
            role="guide",
        ),
        RenderedFile(
            path=root / "CONFIG.sforge.md",
            content=render_starter_config_guide(template.name),
            role="config-guide",
        ),
    ]
    for builder in template.file_builders:
        payload = builder(request)
        path = _resolve_payload_path(root, payload.relative_path)
        rendered.append(
            RenderedFile(
                path=path,
                content=payload.content,
                role=payload.role,
            )
        )
    _reject_duplicate_paths(rendered)
    return tuple(_ordered_files(rendered, config_path))


def _ordered_files(
    files: list[RenderedFile], config_path: Path
) -> tuple[RenderedFile, ...]:
    config = [file for file in files if file.path == config_path]
    other = sorted(
        (file for file in files if file.path != config_path),
        key=lambda file: str(file.path),
    )
    return (*config, *other)


def _reject_duplicate_paths(files: list[RenderedFile]) -> None:
    seen: dict[Path, Path] = {}
    for file in files:
        resolved = file.path.resolve()
        if resolved in seen:
            raise StarterTemplateError(
                f"Starter template rendered duplicate path: {file.path}"
            )
        seen[resolved] = file.path


def _resolve_payload_path(root: Path, relative_path: Path) -> Path:
    if relative_path.is_absolute():
        raise StarterTemplateError(
            f"Starter template path must be relative: {relative_path}"
        )
    if ".." in relative_path.parts:
        raise StarterTemplateError(
            f"Starter template path must stay under output root: {relative_path}"
        )
    resolved = (root / relative_path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise StarterTemplateError(
            f"Starter template path must stay under output root: {relative_path}"
        ) from exc
    return resolved
