from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common import JsonObject


@dataclass(frozen=True)
class RunVariantSpec:
    name: str
    set: JsonObject = field(default_factory=dict)
    axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()


@dataclass(frozen=True)
class RunsSpec:
    type: str = "single"
    axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()
    cases: tuple[RunVariantSpec, ...] = ()
