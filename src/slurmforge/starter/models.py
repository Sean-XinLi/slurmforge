from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FileBuilder = Callable[["InitRequest"], "FilePayload"]
ConfigBuilder = Callable[["InitRequest"], dict[str, Any]]
ReadmeBuilder = Callable[["InitRequest"], "StarterReadmePlan"]


@dataclass(frozen=True)
class StarterTemplate:
    name: str
    description: str
    config_builder: ConfigBuilder
    readme_builder: ReadmeBuilder
    file_builders: tuple[FileBuilder, ...]


@dataclass(frozen=True)
class InitRequest:
    template: str
    output_dir: Path
    force: bool = False


@dataclass(frozen=True)
class GeneratedFile:
    path: Path
    role: str


@dataclass(frozen=True)
class FilePayload:
    relative_path: Path
    content: str
    role: str


@dataclass(frozen=True)
class StarterCommandSet:
    validate: str
    dry_run: str
    submit: str


@dataclass(frozen=True)
class StarterReadmePlan:
    template: str
    commands: StarterCommandSet
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RenderedFile:
    path: Path
    content: str
    role: str


@dataclass(frozen=True)
class StarterWritePlan:
    files: tuple[RenderedFile, ...]
    existing_paths: tuple[Path, ...]


@dataclass(frozen=True)
class InitResult:
    template: str
    output_dir: Path
    config_path: Path
    files: tuple[GeneratedFile, ...]
