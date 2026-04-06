from __future__ import annotations

from dataclasses import dataclass, field


def default_modules() -> list[str]:
    return []


def default_extra_env() -> dict[str, str]:
    return {}


@dataclass(frozen=True)
class EnvConfig:
    modules: list[str] = field(default_factory=default_modules)
    conda_activate: str = ""
    venv_activate: str = ""
    extra_env: dict[str, str] = field(default_factory=default_extra_env)
