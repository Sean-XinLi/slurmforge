from __future__ import annotations

from pathlib import Path

from .catalog import get_template
from .errors import StarterWriteError
from .models import GeneratedFile, InitRequest, InitResult, StarterWritePlan
from .render import render_starter_files


def existing_starter_files(request: InitRequest) -> tuple[Path, ...]:
    return plan_starter_write(request).existing_paths


def plan_starter_write(request: InitRequest) -> StarterWritePlan:
    files = render_starter_files(request, get_template(request.template))
    return StarterWritePlan(
        files=files,
        existing_paths=tuple(file.path for file in files if file.path.exists()),
    )


def create_starter_project(request: InitRequest) -> InitResult:
    config_path = request.output.resolve()
    plan = plan_starter_write(request)
    if plan.existing_paths and not request.force:
        joined = ", ".join(str(path) for path in plan.existing_paths)
        raise StarterWriteError(f"Refusing to overwrite existing files: {joined}. Use --force to replace them.")
    for file in plan.files:
        file.path.parent.mkdir(parents=True, exist_ok=True)
        file.path.write_text(file.content, encoding="utf-8")
    return InitResult(
        template=request.template,
        config_path=config_path,
        files=tuple(GeneratedFile(path=file.path, role=file.role) for file in plan.files),
    )
