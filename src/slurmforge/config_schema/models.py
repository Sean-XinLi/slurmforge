from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfigOption:
    value: str
    description: str


@dataclass(frozen=True)
class ConfigField:
    path: str
    title: str
    short_help: str
    when_to_change: str
    section: str
    level: str
    value_type: str = "value"
    required: bool | None = None
    templates: tuple[str, ...] = ()
    default: str | None = None
    options: tuple[ConfigOption, ...] = ()
    yaml_comment: str | None = None
    validation_scope: str = "config"
    example: str | None = None
    first_edit: bool = False
