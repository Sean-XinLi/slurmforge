from __future__ import annotations

from dataclasses import dataclass, field

from .common import JsonObject


@dataclass(frozen=True)
class EnvironmentSourceSpec:
    path: str
    args: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnvironmentSpec:
    name: str
    modules: tuple[str, ...] = ()
    source: tuple[EnvironmentSourceSpec, ...] = ()
    env: JsonObject = field(default_factory=dict)
